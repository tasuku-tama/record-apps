/**
 * レコーザーアプリ ミドルエンド（現場監督）
 * 役割：録音データの管理、進捗の記憶、GASへの並列送信＆自動リトライ、最終確定
 *
 * 🛠 診断ログ強化対応（今回の変更点）：
 * ・GAS側（コード.gs）で GeminiService.generateContent の戻り値に
 *   finishReason / usage（トークン数）等の診断情報を含めるようにした
 *   ことに合わせ、フロント側でもこれを受け取り、
 *   (a) ブラウザのコンソールに分かりやすく出力する
 *   (b) 「診断情報を表示」パネルにテキストとして表示し、
 *       ワンクリックでクリップボードにコピーできるようにする
 *   の2点を追加した。
 *
 * 🛠 目的：
 *   他の利用者がテストした場合の一次切り分けはGAS側の実行ログ
 *   （Cloud Logging）で行う想定だが、たま氏自身が手元でテストする際は、
 *   ブラウザの開発者ツールを開かずとも、画面上のパネルからそのまま
 *   コピペしてAIに貼れるようにする。
 *
 * 🛠 診断ログ蓄積対応（今回の追加変更点）：
 * ・従来、DiagnosticsReporter.report() は呼ばれるたびにパネルの
 *   テキストエリアを「上書き」していたため、1回のセッションで
 *   複数回レポートが出る場合（例：文字起こし完了時→最終確定時）、
 *   最後に呼ばれたレポートしかパネルに残らなかった。
 *   スマホ（Android等）では開発者ツールのコンソールを開くのが
 *   容易ではないため、手前に出ているはずの文字起こし側の詳細
 *   （finishReason等）が実質確認不能になっていた。
 * ・DiagnosticsReporter に reports 配列を新設し、セッション中に
 *   出た全レポートを蓄積したうえで、結合して表示するように変更した。
 * ・あわせて、finalizeSession が最終確定レポートを出す際、
 *   文字起こし側の診断情報を一律 null で渡していた箇所を、
 *   呼び出し元（transcribeFromGeminiFileUris等）から実データを
 *   引き継げるように修正した。
 */

const CONFIG = {
    GAS_URL: localStorage.getItem('saved_gas_url') || "",
    MAX_RETRIES: 3,
    STORAGE_KEY: "recorder_app_state",
    // 🌟【キュー待ち対応・新設】：
    //   GAS Web Appへの同時アクセス数の上限。Google Apps Scriptには
    //   「1ユーザーあたり同時実行30まで」という制限があり、これを
    //   超えるとリクエスト自体が失敗する。録音中は appendAudioChunk /
    //   uploadChunkToGemini の2系統が並行して走り、リトライも重なる
    //   可能性があるため、クライアント側であらかじめ同時送信数を
    //   絞っておくことで、GAS側の制限に達する前に自然な順番待ちを
    //   発生させる。値は保守的に2としている。
    MAX_CONCURRENT_REQUESTS: 2
};

// ==========================================
// 🌟【キュー待ち対応・新設】：GASへの同時リクエスト数を制限するキュー
//
// 🛠 設計方針：
//   録音中は複数のチャンク（10分ごと）が次々に生成され、それぞれ
//   appendAudioChunk（ドライブ保存）と uploadChunkToGemini（Gemini
//   アップロード）の2系統のリクエストが並行して飛ぶ。長時間録音や
//   再送（リトライ）が重なると、瞬間的に多数のリクエストが同時に
//   GASへ送られる可能性があり、GAS側の同時実行数制限
//   （1ユーザーあたり30）に達するとリクエストが失敗する。
//
//   このキューは、実行中のリクエスト数が MAX_CONCURRENT_REQUESTS を
//   超えないよう、超過分を「空きが出るまで待機」させる。
//   これにより、GAS側の制限に近づく前に、クライアント側で自然な
//   順番待ちを発生させ、失敗ではなく「少し待ってから実行される」
//   という穏やかな挙動にする。
//
//   スコープは「録音中のチャンク送信」のみに限定している（複数の
//   会議・セッションを跨いだキューは、GAS側での非同期ジョブ管理が
//   必要になり大掛かりな改修になるため、別途まとめて検討する）。
// ==========================================
class RequestQueue {
    constructor(maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
        this.running = 0;
        this.waitingQueue = [];
    }

    // 🌟 taskFn（Promiseを返す関数）をキューに投入する。
    //   実行中の件数が上限未満ならすぐに実行し、上限に達している
    //   場合は待機列に積んで、空きが出た順に実行する（先入れ先出し）。
    enqueue(taskFn) {
        return new Promise((resolve, reject) => {
            const run = async () => {
                this.running++;
                try {
                    const result = await taskFn();
                    resolve(result);
                } catch (err) {
                    reject(err);
                } finally {
                    this.running--;
                    this._runNext();
                }
            };

            if (this.running < this.maxConcurrent) {
                run();
            } else {
                this.waitingQueue.push(run);
                console.log(
                    `⏳ RequestQueue: 同時実行数の上限（${this.maxConcurrent}）に達したため、` +
                    `待機列に追加しました。現在の待機数=${this.waitingQueue.length}`
                );
            }
        });
    }

    _runNext() {
        if (this.waitingQueue.length > 0 && this.running < this.maxConcurrent) {
            const next = this.waitingQueue.shift();
            next();
        }
    }
}

// ==========================================
// 🌟 診断情報の整形・保持担当（診断ログ蓄積対応）
//
// 🛠 設計方針：
//   GASから返ってくる diagnostics オブジェクト（Step1文字起こし、
//   Step2校正、議事録生成それぞれの finishReason / usage 等）を
//   受け取り、人間が読みやすいテキストに整形する。
//   1回のセッション中に複数回出るレポートは reports 配列に蓄積し、
//   パネルには常に「その時点までの全レポート」を時系列で表示する。
//   window.lastDiagnosticsText にも保持しておくことで、
//   ブラウザのコンソールから `copy(window.lastDiagnosticsText)` を
//   実行するだけでもクリップボードにコピーできるようにする
//   （UIパネルを使わない場合の保険）。
// ==========================================
const DiagnosticsReporter = {

    // 🌟 このセッション（ページ読み込み〜リロードまで）で出た
    //   レポート文字列を古い順に保持する。
    reports: [],

    // 🌟 diagnostics（Step1/Step2）とtranscribeMultipleFileUrisの
    //   トップレベル情報、および任意でminutesDiagnosticsをまとめて
    //   1つの読みやすいレポート文字列に整形する。
    formatReport: function(label, transcriptResult, minutesDiagnostics) {
        const lines = [];
        lines.push(`===== 診断レポート: ${label} =====`);
        // 🌟【タイムゾーン表記修正】：
        //   従来は toISOString() を使用しており、常にUTC（末尾のZが目印）で
        //   出力されていた。実行環境（Androidスマホ等）の設定に関わらず
        //   UTC固定のため、日本時間として読むと実際の時刻より9時間前の
        //   表記になってしまっていた。GAS側（コード.gs）は
        //   Utilities.formatDate(..., "Asia/Tokyo", ...) で日本時間を
        //   明示している箇所が多いため、フロント側のこの診断ログも
        //   日本時間表記に揃える。
        lines.push(`生成日時: ${new Date().toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' })}`);

        if (transcriptResult) {
            lines.push("");
            lines.push("--- 文字起こし処理 ---");
            if (transcriptResult.fileCount !== undefined) {
                lines.push(`対象ファイル数: ${transcriptResult.fileCount}`);
            }
            if (transcriptResult.finishReason !== undefined) {
                lines.push(`finishReason: ${transcriptResult.finishReason}`);
            }
            if (transcriptResult.outputTextLength !== undefined) {
                lines.push(`出力テキスト文字数: ${transcriptResult.outputTextLength}`);
            }

            const diag = transcriptResult.diagnostics;
            if (diag && diag.step1) {
                lines.push("");
                lines.push("  [Step1: 文字起こし]");
                lines.push(`  finishReason: ${diag.step1.finishReason}`);
                lines.push(`  httpStatus: ${diag.step1.httpStatus}`);
                if (diag.step1.usage) {
                    lines.push(`  promptTokenCount: ${diag.step1.usage.promptTokenCount}`);
                    lines.push(`  candidatesTokenCount: ${diag.step1.usage.candidatesTokenCount}`);
                    lines.push(`  totalTokenCount: ${diag.step1.usage.totalTokenCount}`);
                }
            }
            if (diag && diag.step2) {
                lines.push("");
                lines.push("  [Step2: 校正]");
                lines.push(`  finishReason: ${diag.step2.finishReason}`);
                lines.push(`  httpStatus: ${diag.step2.httpStatus}`);
                if (diag.step2.usage) {
                    lines.push(`  promptTokenCount: ${diag.step2.usage.promptTokenCount}`);
                    lines.push(`  candidatesTokenCount: ${diag.step2.usage.candidatesTokenCount}`);
                    lines.push(`  totalTokenCount: ${diag.step2.usage.totalTokenCount}`);
                }
            }

            if (transcriptResult.finishReason && transcriptResult.finishReason !== "STOP") {
                lines.push("");
                lines.push(`  ⚠️ finishReasonがSTOP以外です（${transcriptResult.finishReason}）。`);
                lines.push(`     文字起こしが音声の途中で打ち切られている可能性があります。`);
            }
        }

        if (minutesDiagnostics) {
            lines.push("");
            lines.push("--- 議事録生成処理 ---");
            lines.push(`finishReason: ${minutesDiagnostics.finishReason}`);
            lines.push(`出力テキスト文字数: ${minutesDiagnostics.outputTextLength}`);
            if (minutesDiagnostics.usage) {
                lines.push(`promptTokenCount: ${minutesDiagnostics.usage.promptTokenCount}`);
                lines.push(`candidatesTokenCount: ${minutesDiagnostics.usage.candidatesTokenCount}`);
                lines.push(`totalTokenCount: ${minutesDiagnostics.usage.totalTokenCount}`);
            }
            if (minutesDiagnostics.finishReason && minutesDiagnostics.finishReason !== "STOP") {
                lines.push("");
                lines.push(`  ⚠️ finishReasonがSTOP以外です（${minutesDiagnostics.finishReason}）。`);
                lines.push(`     議事録が入力データの途中までしか反映されていない可能性があります。`);
            }
        }

        lines.push("");
        lines.push("=================================");

        return lines.join("\n");
    },

    // 🌟 レポートを (a) コンソール出力 (b) reports配列への蓄積
    //   (c) 蓄積済み全レポートのUIパネルへの反映、まとめて行う。
    //
    // 🛠 UI崩れ対策（レビュー後の調整）：
    //   当初は毎回の完了時にパネルを強制的に開いていたが、これだと
    //   正常終了時にも画面がガチャッと切り替わって煩雑に見えるため、
    //   「テキストの中身は常に最新化する」が「パネルを勝手に開くのは
    //   異常時（finishReasonがSTOP以外）のみ」に変更した。
    //   正常時はボタン（🩺診断）を押した時にだけ表示すれば十分。
    //
    // 🛠 診断ログ蓄積対応（今回の変更）：
    //   スマホ等で開発者ツールのコンソールを開きにくい環境でも、
    //   1回のセッション中に出た複数のレポート（文字起こし完了時、
    //   最終確定時など）をすべてパネル上で遡って確認できるよう、
    //   テキストエリアには「蓄積済み全レポートを結合したもの」を
    //   常に反映するようにした。
    report: function(label, transcriptResult, minutesDiagnostics) {
        const reportText = this.formatReport(label, transcriptResult, minutesDiagnostics);

        // (a) コンソールへ出力（開発者ツールでそのまま選択・コピー可能）
        console.log(reportText);

        // 🌟 (b) 蓄積配列へ追加。セッション（ページの読み込み）中の全レポートを保持する。
        this.reports.push(reportText);

        // 🌟 (c) 蓄積された全レポートを結合し、件数の案内を先頭に付けたものを
        //   window.lastDiagnosticsText と UIパネルの両方に反映する。
        //   古い順→新しい順に並べ、最新のものが一番下に来るようにする。
        const combinedText =
            `【${this.reports.length}件のレポートを表示中（古い順・最新は末尾）】\n\n` +
            this.reports.join("\n\n");

        // グローバル変数へ保持（コンソールから copy(window.lastDiagnosticsText) 可能）
        window.lastDiagnosticsText = combinedText;

        // UIパネルのテキストは常に最新化しておく（表示するかは別判断）
        const textarea = document.getElementById('diagnosticsText');
        if (textarea) {
            textarea.value = combinedText;
            // 🌟 最新のレポートがすぐ見えるよう、スクロール位置を末尾に合わせる
            textarea.scrollTop = textarea.scrollHeight;
        }

        // 🌟 異常（finishReasonがSTOP以外）を検知した場合のみ、
        //   ユーザーが気づけるようパネルを自動的に開く。
        const hasAbnormalFinish =
            (transcriptResult && transcriptResult.finishReason && transcriptResult.finishReason !== "STOP") ||
            (minutesDiagnostics && minutesDiagnostics.finishReason && minutesDiagnostics.finishReason !== "STOP");

        if (hasAbnormalFinish) {
            const wrapper = document.getElementById('diagnosticsPanel');
            const btn = document.getElementById('showDiagnosticsBtn');
            if (wrapper) {
                wrapper.style.display = 'block';
                wrapper.classList.add('fade-in');
            }
            if (btn) {
                btn.classList.add('active-panel');
            }
        }

        return combinedText;
    },

    // 🌟 新規セッション開始時（録音開始時）など、明示的に呼ばれたときのみ
    //   蓄積済みレポートをクリアするヘルパー。
    //   あえて録音開始時などに自動で組み込まず、必要になったら
    //   呼び出し側から明示的に呼ぶ設計とし、「前回の録音の診断ログを
    //   見たかったのに消えていた」という事故を防ぐ。
    clearReports: function() {
        this.reports = [];
        window.lastDiagnosticsText = "";
        const textarea = document.getElementById('diagnosticsText');
        if (textarea) {
            textarea.value = "";
        }
    },

    // 🌟【デバッグ機能の恒久化・統合対応】：
    //   callGasApi でGAS通信が失敗した際（JSONパース失敗、
    //   data.success:false 等）の詳細を、アラートで都度中断させる
    //   のではなく、他の診断レポートと同じ形式で reports 配列に
    //   蓄積する。これにより、録音中の操作は止めずに、後から
    //   🩺診断パネルで通信エラーの詳細（action名・httpStatus・
    //   エラー内容・レスポンス本文の先頭部分）を確認できるように
    //   する。異常発生時のパネル自動オープンは report() 側の
    //   ロジックと同様に行う。
    reportCommError: function(action, httpStatus, errorSummary, rawSnippet) {
        const lines = [];
        lines.push(`===== GAS通信エラー: ${action} =====`);
        lines.push(`発生日時: ${new Date().toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' })}`);
        lines.push(`httpStatus: ${httpStatus}`);
        lines.push(`エラー内容: ${errorSummary}`);
        if (rawSnippet) {
            lines.push(`レスポンス本文(先頭300文字): ${rawSnippet}`);
        }
        lines.push("=================================");
        const reportText = lines.join("\n");

        console.error(reportText);
        this.reports.push(reportText);

        const combinedText =
            `【${this.reports.length}件のレポートを表示中（古い順・最新は末尾）】\n\n` +
            this.reports.join("\n\n");
        window.lastDiagnosticsText = combinedText;

        const textarea = document.getElementById('diagnosticsText');
        if (textarea) {
            textarea.value = combinedText;
            textarea.scrollTop = textarea.scrollHeight;
        }

        // 🌟 通信エラーは常に「異常」なので、診断パネルを自動的に開く。
        const wrapper = document.getElementById('diagnosticsPanel');
        const btn = document.getElementById('showDiagnosticsBtn');
        if (wrapper) {
            wrapper.style.display = 'block';
            wrapper.classList.add('fade-in');
        }
        if (btn) {
            btn.classList.add('active-panel');
        }
    }
};

class RecorderController {
    constructor(config) {
        this.gasUrl = config.GAS_URL;
        this.maxRetries = config.MAX_RETRIES;
        this.storageKey = config.STORAGE_KEY;
        this.state = this.loadState() || this.createInitialState();

        this.isProcessing = false;
        this.hasFinalized = false;

        this.isFinalizingAudio = false;

        this.audioAppendFailures = [];

        // 🌟【キュー待ち対応・新設】：GASへの同時リクエスト数を制限するキュー。
        this.requestQueue = new RequestQueue(config.MAX_CONCURRENT_REQUESTS || 2);
    }

    // --- 部署1: 記憶・進捗管理担当 ---
    createInitialState() {
        return {
            sessionId: Date.now().toString(),
            mode: '3',
            chunks: [],
            isCompleted: false,
            audioFinalized: false,
            audioUrl: null,
            audioFolderId: null,
            mimeType: null,
            geminiFileUris: [],
            geminiUploadFailures: [],
            expectedChunkCount: 0
        };
    }

    saveState() { 
        const cleanChunks = this.state.chunks.map(chunk => {
            const { base64Data, ...rest } = chunk;
            return rest;
        });
        
        const stateCopy = {
            ...this.state,
            chunks: cleanChunks
        };
        
        localStorage.setItem(this.storageKey, JSON.stringify(stateCopy)); 
    }

    loadState() {
        const saved = localStorage.getItem(this.storageKey);
        return saved ? JSON.parse(saved) : null;
    }
    
    clearState() {
        localStorage.removeItem(this.storageKey);
        this.state = this.createInitialState();
        this.hasFinalized = false;
        this.audioAppendFailures = [];
    }

    startNewSession() {
        this.state = this.createInitialState();
        this.hasFinalized = false;
        this.isProcessing = false;
        this.isFinalizingAudio = false;
        this.audioAppendFailures = [];
        this.saveState();
        return this.state.sessionId;
    }

    addTranscribeChunk(base64Data, index, mimeType, meetingName) {
        if (this.hasFinalized) {
            console.warn(`⚠️ addTranscribeChunk: セッション確定後のチャンク(${index})を検知、破棄します。`);
            return;
        }

        this.state.chunks.push({
            id: index,
            base64Data: base64Data, 
            mimeType: mimeType || 'audio/webm',
            meetingName: meetingName || "無題の会議",
            status: 'pending',
            text: '',
            retryCount: 0
        });
        this.saveState();
    }

    async appendAudioChunk(base64Data, mimeType, meetingName, chunkIndex) {
        if (!this.state.mimeType) {
            this.state.mimeType = mimeType || 'audio/webm';
        }

        if (chunkIndex + 1 > this.state.expectedChunkCount) {
            this.state.expectedChunkCount = chunkIndex + 1;
            this.saveState();
        }

        const payload = {
            action: "appendAudioChunk",
            sessionId: this.state.sessionId,
            audioData: base64Data,
            mimeType: mimeType || 'audio/webm',
            meetingName: meetingName || "無題の会議",
            chunkIndex: chunkIndex
        };

        let lastError = null;
        for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    const waitTime = Math.pow(2, attempt) * 1000;
                    console.warn(`⚠️ 音声チャンク保存: ${waitTime / 1000}秒待機して再送します...`);
                    await new Promise(res => setTimeout(res, waitTime));
                }
                await this.callGasApi(payload);
                return true;
            } catch (error) {
                lastError = error;
                console.error(`❌ 音声チャンク保存エラー (${attempt + 1}回目)`, error);
            }
        }

        this.audioAppendFailures.push({ chunkIndex, error: lastError ? lastError.message : "unknown" });
        return false;
    }

    async uploadChunkToGeminiAndStore(base64Data, mimeType, chunkIndex) {
        if (chunkIndex + 1 > this.state.expectedChunkCount) {
            this.state.expectedChunkCount = chunkIndex + 1;
            this.saveState();
        }

        const payload = {
            action: "uploadChunkToGemini",
            audioData: base64Data,
            mimeType: mimeType || 'audio/webm',
            chunkIndex: chunkIndex
        };

        let lastError = null;
        for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    const waitTime = Math.pow(2, attempt) * 1000;
                    console.warn(`⚠️ Geminiアップロード: ${waitTime / 1000}秒待機して再送します...（チャンク${chunkIndex}）`);
                    await new Promise(res => setTimeout(res, waitTime));
                }
                const response = await this.callGasApi(payload);

                this.state.geminiFileUris.push({
                    chunkIndex: chunkIndex,
                    fileUri: response.fileUri
                });
                this.saveState();

                console.log(`✅ Geminiアップロード完了（チャンク${chunkIndex}）: fileUri=${response.fileUri}`);
                return true;

            } catch (error) {
                lastError = error;
                console.error(`❌ Geminiアップロードエラー (${attempt + 1}回目、チャンク${chunkIndex})`, error);
            }
        }

        this.state.geminiUploadFailures.push({ chunkIndex, error: lastError ? lastError.message : "unknown" });
        this.saveState();
        console.error(`❌ チャンク${chunkIndex}のGeminiアップロードが最終的に失敗しました。このチャンクの内容は文字起こしに含まれません。`);
        return false;
    }

    async finalizeAudioAndGetResult(meetingName) {
        if (this.state.audioFinalized) {
            console.log("ℹ️ finalizeAudioAndGetResult: この録音は既に音声確定済みです。スキップします。");
            return {
                audioFinalized: true,
                audioUrl: this.state.audioUrl,
                audioFolderId: this.state.audioFolderId,
                alreadyFinalized: true
            };
        }

        if (this.isFinalizingAudio) {
            console.warn("⚠️ finalizeAudioAndGetResult: 既に音声確定処理が進行中です。");
            return null;
        }
        this.isFinalizingAudio = true;

        const payload = {
            action: "finalizeAudio",
            sessionId: this.state.sessionId,
            meetingName: meetingName || "無題の会議",
            mimeType: this.state.mimeType || 'audio/webm'
        };

        const audioFinalizeMaxRetries = this.maxRetries + 2;
        let lastError = null;

        try {
            for (let attempt = 0; attempt <= audioFinalizeMaxRetries; attempt++) {
                try {
                    if (attempt > 0) {
                        const waitTime = Math.pow(2, attempt) * 1000;
                        console.warn(`⚠️ 音声確定処理: ${waitTime / 1000}秒待機して再試行します... (${attempt}回目)`);
                        await new Promise(res => setTimeout(res, waitTime));
                    }

                    const response = await this.callGasApi(payload);

                    this.state.audioFinalized = true;
                    this.state.audioUrl = response.audioUrl || null;
                    this.state.audioFolderId = response.audioFolderId || null;
                    this.saveState();

                    console.log("✅ 音声データの確認が完了しました（案A：連番フォルダ保存）。", response);

                    return {
                        audioFinalized: response.audioFinalized !== false,
                        audioUrl: response.audioUrl || null,
                        audioFolderId: response.audioFolderId || null,
                        fileCount: response.fileCount,
                        alreadyFinalized: !!response.alreadyFinalized
                    };

                } catch (error) {
                    lastError = error;
                    console.error(`❌ 音声確定処理エラー (${attempt + 1}回目)`, error);
                }
            }

            throw new Error(
                "音声データの確認に失敗しました。録音の各セグメントはGoogleドライブの" +
                "「レコーダー/音声データ」フォルダ内の、会議名のついた専用フォルダに" +
                "個別に残っている可能性があります。手動での確認をお願いします。詳細: " +
                (lastError ? lastError.message : "不明なエラー")
            );

        } finally {
            this.isFinalizingAudio = false;
        }
    }

    async transcribeFinalizedAudio(meetingName, templateType) {
        if (this.isProcessing) {
            console.warn("⚠️ transcribeFinalizedAudio: 既に処理中のため、この呼び出しは無視します。");
            return null;
        }
        if (this.hasFinalized) {
            console.warn("⚠️ transcribeFinalizedAudio: このセッションは既に確定済みのため、この呼び出しは無視します。");
            return null;
        }
        if (!this.state.audioFolderId) {
            throw new Error(
                "音声の専用フォルダがまだ確認できていません。先に音声データの確認を完了させてください。"
            );
        }

        this.isProcessing = true;

        try {
            console.log("🚀 専用フォルダ内の音声ファイルからの文字起こしを開始します...", "audioFolderId:", this.state.audioFolderId);

            const payload = {
                action: "transcribeFromAudioFile",
                audioFolderId: this.state.audioFolderId,
                mimeType: this.state.mimeType || 'audio/webm'
            };

            let lastError = null;
            let response = null;
            for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
                try {
                    if (attempt > 0) {
                        const waitTime = Math.pow(2, attempt) * 1000;
                        console.warn(`⚠️ 文字起こし処理: ${waitTime / 1000}秒待機して再試行します... (${attempt}回目)`);
                        await new Promise(res => setTimeout(res, waitTime));
                    }
                    response = await this.callGasApi(payload);
                    break;
                } catch (error) {
                    lastError = error;
                    console.error(`❌ 文字起こし処理エラー (${attempt + 1}回目)`, error);
                }
            }

            if (!response) {
                throw new Error(
                    "文字起こし処理に失敗しました。音声データ自体は専用フォルダに保存済みのため失われていません。" +
                    "詳細: " + (lastError ? lastError.message : "不明なエラー")
                );
            }

            // 🌟【診断ログ強化対応・追加】：GASから返ってきた診断情報をレポートする
            DiagnosticsReporter.report("専用フォルダからの文字起こし再開", response, null);

            const fullTranscript = response.text || "";
            console.log("🎉 文字起こしが完了しました！");

            // 🌟【診断ログ蓄積対応・変更】：
            //   文字起こし側の診断情報（response）をfinalizeSessionまで
            //   引き継ぎ、最終確定レポートでもnull扱いにならないようにする。
            return await this.finalizeSession(meetingName, templateType, fullTranscript, response);

        } finally {
            this.isProcessing = false;
        }
    }

    detectMissingChunks() {
        const expectedCount = this.state.expectedChunkCount || 0;
        if (expectedCount === 0) {
            return [];
        }

        const presentIndexes = new Set(
            (this.state.geminiFileUris || []).map(entry => entry.chunkIndex)
        );

        const missing = [];
        for (let i = 0; i < expectedCount; i++) {
            if (!presentIndexes.has(i)) {
                missing.push(i);
            }
        }
        return missing;
    }

    async transcribeFromGeminiFileUris(meetingName, templateType) {
        if (this.isProcessing) {
            console.warn("⚠️ transcribeFromGeminiFileUris: 既に処理中のため、この呼び出しは無視します。");
            return null;
        }
        if (this.hasFinalized) {
            console.warn("⚠️ transcribeFromGeminiFileUris: このセッションは既に確定済みのため、この呼び出しは無視します。");
            return null;
        }
        if (!this.state.geminiFileUris || this.state.geminiFileUris.length === 0) {
            throw new Error(
                "文字起こし対象の音声データがGeminiにアップロードされていません。" +
                "録音が正しく行われたかご確認ください。"
            );
        }

        const missingChunks = this.detectMissingChunks();
        if (missingChunks.length > 0) {
            console.warn(
                `⚠️ チャンク欠番を検知しました: [${missingChunks.join(", ")}]。` +
                `該当区間は文字起こしに反映されません（音声原本はDriveに残っています）。`
            );
        }

        this.isProcessing = true;

        try {
            const sortedEntries = [...this.state.geminiFileUris].sort(
                (a, b) => a.chunkIndex - b.chunkIndex
            );
            const orderedFileUris = sortedEntries.map(entry => entry.fileUri);

            console.log(
                "🚀 逐次アップロード済みのfileUri配列から文字起こしを開始します...",
                "件数:", orderedFileUris.length
            );

            const payload = {
                action: "transcribeFromMultipleFiles",
                fileUris: orderedFileUris,
                mimeType: this.state.mimeType || 'audio/webm'
            };

            let lastError = null;
            let response = null;
            for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
                try {
                    if (attempt > 0) {
                        const waitTime = Math.pow(2, attempt) * 1000;
                        console.warn(`⚠️ 文字起こし処理: ${waitTime / 1000}秒待機して再試行します... (${attempt}回目)`);
                        await new Promise(res => setTimeout(res, waitTime));
                    }
                    response = await this.callGasApi(payload);
                    break;
                } catch (error) {
                    lastError = error;
                    console.error(`❌ 文字起こし処理エラー (${attempt + 1}回目)`, error);
                }
            }

            if (!response) {
                throw new Error(
                    "文字起こし処理に失敗しました。音声データ自体はDriveに確定保存済み（または保存処理中）のため失われていません。" +
                    "詳細: " + (lastError ? lastError.message : "不明なエラー")
                );
            }

            // 🌟【診断ログ強化対応・追加】：
            //   通常フローの主経路であり、今回の不具合が最も疑われる箇所。
            //   ここで必ず診断レポートを出す。
            DiagnosticsReporter.report("通常フロー（逐次アップロード済みfileUriからの文字起こし）", response, null);

            const fullTranscript = response.text || "";
            console.log("🎉 文字起こしが完了しました！");

            // 🌟【診断ログ蓄積対応・変更】：
            //   文字起こし側の診断情報（response、finishReasonを含む）を
            //   finalizeSessionまで引き継ぐ。これにより「最終確定」レポート内の
            //   文字起こし処理欄が常にnullになっていた問題を解消する。
            return await this.finalizeSession(meetingName, templateType, fullTranscript, response);

        } finally {
            this.isProcessing = false;
        }
    }

    async processAllChunks(meetingName, templateType, onProgressCallback) {
        if (this.isProcessing) {
            console.warn("⚠️ processAllChunks: 既に処理中のため、この呼び出しは無視します。");
            return null;
        }
        if (this.hasFinalized) {
            console.warn("⚠️ processAllChunks: このセッションは既に確定済みのため、この呼び出しは無視します。");
            return null;
        }
        this.isProcessing = true;

        try {
            console.log("🚀 文字起こし処理を開始します...", "chunks数:", this.state.chunks.length);
            const pendingChunks = this.state.chunks.filter(c => c.status !== 'completed');

            pendingChunks.forEach(chunk => {
                if (!chunk.meetingName) chunk.meetingName = meetingName || "無題の会議";
            });

            const promises = pendingChunks.map(chunk => 
                this.sendChunkWithRetry(chunk, onProgressCallback)
            );
            await Promise.all(promises);

            const allCompleted = this.state.chunks.every(c => c.status === 'completed');
            if (allCompleted) {
                console.log("🎉 すべての文字起こしが完了しました！");
                const sortedChunks = [...this.state.chunks].sort((a, b) => a.id - b.id);
                const fullTranscript = sortedChunks.map(c => c.text).join('\n\n');
                // 🌟 このフロー（旧・チャンク並列送信方式）はチャンクごとのレスポンスに
                //   finishReason等の診断情報オブジェクトを保持していないため、
                //   従来通り診断情報なし（undefined）でfinalizeSessionを呼ぶ。
                //   finalizeSession側でフォールバック処理される。
                return await this.finalizeSession(meetingName, templateType, fullTranscript);
            } else {
                throw new Error("一部の処理がエラーで停止しました。手動再開をお願いします。");
            }
        } finally {
            this.isProcessing = false;
        }
    }

    async sendChunkWithRetry(chunk, onProgressCallback) {
        chunk.status = 'processing';
        this.saveState();
        if(onProgressCallback) onProgressCallback(this.state.chunks);

        while (chunk.retryCount <= this.maxRetries) {
            try {
                const waitTime = chunk.retryCount === 0 ? 0 : Math.pow(2, chunk.retryCount) * 1000;
                if (waitTime > 0) {
                    console.warn(`⚠️ パーツ${chunk.id}: ${waitTime/1000}秒待機して再送します...`);
                    await new Promise(res => setTimeout(res, waitTime));
                }

                const payload = {
                    action: "transcribe",
                    meetingName: chunk.meetingName,
                    chunkId: chunk.id,
                    audioData: chunk.base64Data,
                    mimeType: chunk.mimeType 
                };
                
                const response = await this.callGasApi(payload);
                
                chunk.status = 'completed';
                chunk.text = response.text;
                this.saveState();
                if(onProgressCallback) onProgressCallback(this.state.chunks);
                return true;

            } catch (error) {
                chunk.retryCount++;
                console.error(`❌ パーツ${chunk.id}: エラー (${chunk.retryCount}回目)`, error);
                
                if (chunk.retryCount > this.maxRetries) {
                    chunk.status = 'error';
                    this.saveState();
                    if(onProgressCallback) onProgressCallback(this.state.chunks);
                    return false;
                }
            }
        }
    }

    // 🌟【デバッグ機能の恒久化・統合対応】：
    //   GASとの通信に失敗した場合（JSONパース失敗、data.success:false等）、
    //   従来は原因調査のためアラートで即座に中断表示していたが、
    //   原因調査が完了したため、操作を妨げないログ記録方式に統合した。
    //   詳細は DiagnosticsReporter.reportCommError() に記録され、
    //   🩺診断パネルから後で確認できる（異常時は自動的にパネルが開く）。
    //
    // 🌟【キュー待ち対応・変更点】：
    //   実際の通信処理（fetch〜レスポンス解析）を requestQueue.enqueue()
    //   でラップした。これにより、callGasApi を呼び出す全ての箇所
    //   （appendAudioChunk, uploadChunkToGeminiAndStore,
    //   finalizeAudioAndGetResult 等）が自動的に同時実行数制限の
    //   恩恵を受ける。呼び出し元のコードは一切変更不要。
    async callGasApi(payload) {
        if (!this.gasUrl) throw new Error("GASのURLが設定されていません。");

        return this.requestQueue.enqueue(async () => {
            const response = await fetch(this.gasUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'text/plain' },
                body: JSON.stringify(payload)
            });

            const rawText = await response.text();

            let data;
            try {
                data = JSON.parse(rawText);
            } catch (parseErr) {
                DiagnosticsReporter.reportCommError(
                    payload.action,
                    response.status,
                    "GASレスポンスのJSONパースに失敗しました。",
                    rawText.substring(0, 300)
                );
                throw new Error("GASレスポンスのJSONパースに失敗しました。httpStatus=" + response.status);
            }

            if (!data.success) {
                DiagnosticsReporter.reportCommError(
                    payload.action,
                    response.status,
                    data.error || "(GAS側からエラー内容が返されませんでした)"
                );
                throw new Error(data.error || "GAS側で不明なエラーが発生しました");
            }
            return data.data;
        });
    }

    hasPendingSession() {
        return !!(this.state && this.state.sessionId && this.state.chunks);
    }

    hasPendingGeminiFileUris() {
        return !!(this.state && this.state.geminiFileUris && this.state.geminiFileUris.length > 0);
    }

    async checkSessionStatus() {
        if (!this.state.sessionId) {
            return { status: "not_found" };
        }
        const payload = {
            action: "checkSessionStatus",
            sessionId: this.state.sessionId
        };
        return await this.callGasApi(payload);
    }

    async resumeFromAudioPending(meetingName) {
        console.log("🛠 お助けモード：音声結合の再開を試みます。sessionId=", this.state.sessionId);
        return await this.finalizeAudioAndGetResult(meetingName);
    }

    async resumeFromTranscriptPending(audioFolderId, mimeType, meetingName, templateType) {
        console.log("🛠 お助けモード：専用フォルダ内の音声ファイルからの文字起こし再開を試みます。audioFolderId=", audioFolderId);

        const payload = {
            action: "transcribeFromAudioFile",
            audioFolderId: audioFolderId,
            mimeType: mimeType || 'audio/webm'
        };

        const response = await this.callGasApi(payload);

        // 🌟【診断ログ強化対応・追加】
        DiagnosticsReporter.report("お助けモード：専用フォルダからの文字起こし再開", response, null);

        const fullTranscript = response.text || "";

        const minutesPayload = {
            action: "generateMinutes",
            text: fullTranscript,
            meetingName: meetingName || "無題の会議",
            templateType: templateType || "汎用議事録",
            mode: this.state.mode || "3"
        };

        const minutesResponse = await this.callGasApi(minutesPayload);

        // 🌟【診断ログ強化対応・追加】：議事録生成の診断情報も追記でレポートする
        //   🛠【診断ログ蓄積対応・変更】：ここでもresponse（文字起こし側のfinishReason等）を
        //   そのまま引き継いでレポートし、nullにならないようにする。
        if (minutesResponse.minutesDiagnostics) {
            DiagnosticsReporter.report(
                "お助けモード：議事録生成",
                response,
                minutesResponse.minutesDiagnostics
            );
        }

        this.clearState();

        return {
            transcript: fullTranscript,
            documentUrl: minutesResponse.documentUrl,
            transcriptUrl: minutesResponse.transcriptUrl
        };
    }

    // 🌟【診断ログ蓄積対応・変更】：
    //   第4引数 transcriptDiagnosticsSource を新設。
    //   呼び出し元（transcribeFromGeminiFileUris / transcribeFinalizedAudio等）から
    //   文字起こし処理のレスポンス（finishReason等を含む）を受け取れるようにし、
    //   最終確定レポート内の「文字起こし処理」欄が常にnullになっていた問題を解消する。
    //   呼び出し元が渡してこない場合（processAllChunks等の旧経路）は、
    //   従来通りnull扱いにフォールバックする。
    async finalizeSession(meetingName, templateType, fullTranscript, transcriptDiagnosticsSource) {
        if (this.hasFinalized) {
            console.warn("⚠️ finalizeSession: 既に確定済みのため、この呼び出しは無視します。");
            return null;
        }
        this.hasFinalized = true;

        console.log("🔗 文字起こしの結合と最終確定を開始します...", new Date().toISOString());

        let resultData = {
            transcript: fullTranscript,
            documentUrl: null,
            transcriptUrl: null,
            audioFinalized: this.state.audioFinalized,
            audioUrl: this.state.audioUrl
        };

        const payload = {
            action: "generateMinutes",
            text: fullTranscript,
            meetingName: meetingName || "無題の会議",
            templateType: templateType || "汎用議事録",
            mode: this.state.mode
        };

        try {
            const response = await this.callGasApi(payload);
            resultData.documentUrl = response.documentUrl;
            resultData.transcriptUrl = response.transcriptUrl;
            console.log("✅ 最終処理完了: ", response);

            // 🌟【診断ログ強化対応・追加】：
            //   議事録生成側の診断情報も、直前の文字起こし診断情報と
            //   合わせて再レポートする（1回のセッションの最終報告として、
            //   まとめて確認できるようにするため）。
            //
            // 🛠【診断ログ蓄積対応・変更】：
            //   従来 { finishReason: null, diagnostics: null } という
            //   ダミーを渡していた箇所を、呼び出し元から引き継いだ
            //   transcriptDiagnosticsSource に差し替える。
            if (response.minutesDiagnostics) {
                DiagnosticsReporter.report(
                    "最終確定（文字起こし＋議事録生成）",
                    transcriptDiagnosticsSource || { finishReason: null, diagnostics: null },
                    response.minutesDiagnostics
                );
            }

            if (this.audioAppendFailures.length > 0) {
                console.warn("⚠️ 録音中に一部の音声追記保存が失敗していました。音声に欠損がある可能性があります。", this.audioAppendFailures);
                resultData.audioWarning = true;
            }

            const failureIndexes = (this.state.geminiUploadFailures || []).map(f => f.chunkIndex);
            const detectedMissing = this.detectMissingChunks();
            const missingChunkIndexes = [...new Set([...failureIndexes, ...detectedMissing])].sort((a, b) => a - b);

            if (missingChunkIndexes.length > 0) {
                console.warn(
                    "⚠️ 録音中に一部のチャンクが欠落していました。" +
                    "該当区間の内容は文字起こし・議事録に反映されていません。",
                    missingChunkIndexes
                );
                resultData.transcriptionWarning = true;
                resultData.missingChunkIndexes = missingChunkIndexes;
            }

            this.clearState();
            return resultData;
        } catch (err) {
            console.error("❌ 最終確定処理でエラー発生。再試行できるようフラグを戻します。", err);
            this.hasFinalized = false;
            throw err;
        }
    }
}

// 🚀 現場監督の出勤
window.appController = new RecorderController(CONFIG);
