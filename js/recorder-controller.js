/**
 * レコーザーアプリ ミドルエンド（現場監督）
 * 役割：録音データの管理、進捗の記憶、GASへの送信（同時実行数の制限＋自動リトライ）、音声の確定保存
 *
 * 🛠 診断ログ強化対応：
 * ・GAS側（コード.gs）で GeminiService.generateContent の戻り値に
 *   finishReason / usage（トークン数）等の診断情報を含めるようにしたことに
 *   合わせ、フロント側でもこれを受け取り、
 *   (a) ブラウザのコンソールに分かりやすく出力する
 *   (b) 「診断情報を表示」パネルにテキストとして表示し、
 *       ワンクリックでクリップボードにコピーできるようにする
 *   の2点を追加した。
 *
 * 🛠 診断ログ蓄積対応：
 * ・DiagnosticsReporter に reports 配列を設け、セッション中に出た全レポートを
 *   蓄積して結合表示する。
 *
 * 🛠 修正内容（2026-10-19・バックグラウンド処理方式への整理）：
 * ・文字起こし・議事録生成は、GAS側の時間主導型トリガー（processQueueTrigger）が
 *   Driveに保存された音声ファイルから自動的に処理する。このため、フロントは
 *   「音声のDrive保存（appendAudioChunk）」と「確定（finalizeAudio）」のみを行う。
 *   録音中のGeminiへの直接アップロード、画面側での文字起こし・議事録生成、
 *   旧お助けモードの再開処理（いずれも現在は使われない）を削除した。
 * ・loadState：保存データが壊れている場合に例外でアプリ全体が止まらないよう、
 *   try/catch で保護し、必要な項目だけを取り込むようにした。
 * ・hasPendingSession：clearState 後でも常に true になっていた判定を、
 *   「1件以上のチャンクを送信した録音がある」場合のみ true に修正した。
 * ・finalizeAudioAndGetResult：サーバー側にセッションフォルダが見つからず
 *   audioFinalized:false が返った場合に、確定済みとして状態を保存してしまわないよう修正した。
 */

const CONFIG = {
    GAS_URL: localStorage.getItem('saved_gas_url') || "",
    MAX_RETRIES: 3,
    STORAGE_KEY: "recorder_app_state",
    // 🌟 GAS Web Appへの同時アクセス数の上限。
    //   Google Apps Scriptには「1ユーザーあたり同時実行30まで」という制限があり、
    //   これを超えるとリクエスト自体が失敗する。クライアント側であらかじめ
    //   同時送信数を絞っておくことで、制限に達する前に自然な順番待ちを発生させる。
    MAX_CONCURRENT_REQUESTS: 2
};

// ==========================================
// 🌟 GASへの同時リクエスト数を制限するキュー
//
// 🛠 設計方針：
//   録音中は複数のチャンクが次々に生成され、リトライも重なる可能性があるため、
//   実行中のリクエスト数が maxConcurrent を超えないよう、超過分を
//   「空きが出るまで待機」させる（先入れ先出し）。
// ==========================================
class RequestQueue {
    constructor(maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
        this.running = 0;
        this.waitingQueue = [];
    }

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
// 🌟 診断情報の整形・保持担当
//
// 🛠 設計方針：
//   GASから返ってくる diagnostics オブジェクト（Step1文字起こし、
//   Step2校正、議事録生成それぞれの finishReason / usage 等）や、
//   通信エラーの詳細を、人間が読みやすいテキストに整形して蓄積する。
//   window.lastDiagnosticsText にも保持しておくことで、
//   ブラウザのコンソールから `copy(window.lastDiagnosticsText)` を
//   実行するだけでもクリップボードにコピーできる（UIパネルを使わない場合の保険）。
// ==========================================
const DiagnosticsReporter = {

    // 🌟 このセッション（ページ読み込み〜リロードまで）で出たレポート文字列を古い順に保持する。
    reports: [],

    formatReport: function(label, transcriptResult, minutesDiagnostics) {
        const lines = [];
        lines.push(`===== 診断レポート: ${label} =====`);
        // 🌟 日本時間で表記する（toISOString() は常にUTCになるため使用しない）。
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

    // 🌟 蓄積済みの全レポートを結合し、グローバル変数とUIパネルに反映する共通処理。
    _refreshPanel: function() {
        const combinedText =
            `【${this.reports.length}件のレポートを表示中（古い順・最新は末尾）】\n\n` +
            this.reports.join("\n\n");

        window.lastDiagnosticsText = combinedText;

        const textarea = document.getElementById('diagnosticsText');
        if (textarea) {
            textarea.value = combinedText;
            textarea.scrollTop = textarea.scrollHeight;
        }
        return combinedText;
    },

    _openPanel: function() {
        const wrapper = document.getElementById('diagnosticsPanel');
        const btn = document.getElementById('showDiagnosticsBtn');
        if (wrapper) {
            wrapper.style.display = 'block';
            wrapper.classList.add('fade-in');
        }
        if (btn) {
            btn.classList.add('active-panel');
        }
    },

    // 🌟 レポートを (a) コンソール出力 (b) reports配列への蓄積 (c) UIパネルへの反映。
    //   パネルを勝手に開くのは異常時（finishReasonがSTOP以外）のみ。
    report: function(label, transcriptResult, minutesDiagnostics) {
        const reportText = this.formatReport(label, transcriptResult, minutesDiagnostics);

        console.log(reportText);
        this.reports.push(reportText);
        const combinedText = this._refreshPanel();

        const hasAbnormalFinish =
            (transcriptResult && transcriptResult.finishReason && transcriptResult.finishReason !== "STOP") ||
            (minutesDiagnostics && minutesDiagnostics.finishReason && minutesDiagnostics.finishReason !== "STOP");

        if (hasAbnormalFinish) {
            this._openPanel();
        }

        return combinedText;
    },

    // 🌟 蓄積済みレポートをクリアするヘルパー（必要なときに呼び出し側から明示的に呼ぶ）。
    clearReports: function() {
        this.reports = [];
        window.lastDiagnosticsText = "";
        const textarea = document.getElementById('diagnosticsText');
        if (textarea) {
            textarea.value = "";
        }
    },

    // 🌟 GAS通信が失敗した際（JSONパース失敗、data.success:false 等）の詳細を、
    //   操作を止めずに reports 配列へ蓄積する。後から🩺診断パネルで確認できる。
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
        this._refreshPanel();

        // 通信エラーは常に「異常」なので、診断パネルを自動的に開く。
        this._openPanel();
    }
};

class RecorderController {
    constructor(config) {
        this.gasUrl = config.GAS_URL;
        this.maxRetries = config.MAX_RETRIES;
        this.storageKey = config.STORAGE_KEY;
        this.state = this.loadState() || this.createInitialState();

        this.isFinalizingAudio = false;

        this.audioAppendFailures = [];

        // 🌟 GASへの同時リクエスト数を制限するキュー。
        this.requestQueue = new RequestQueue(config.MAX_CONCURRENT_REQUESTS || 2);
    }

    // --- 部署1: 記憶・進捗管理担当 ---
    createInitialState() {
        return {
            sessionId: Date.now().toString(),
            mode: '3',
            audioFinalized: false,
            audioUrl: null,
            audioFolderId: null,
            mimeType: null,
            expectedChunkCount: 0
        };
    }

    saveState() {
        try {
            localStorage.setItem(this.storageKey, JSON.stringify(this.state));
        } catch (e) {
            console.warn("⚠️ 状態の保存に失敗しました（無視して続行）:", e);
        }
    }

    // 🌟 保存データが壊れている場合でもアプリ全体が止まらないよう、
    //   try/catch で保護する。古い形式（chunks / geminiFileUris 等を含む）の
    //   保存データからは、現在使う項目だけを取り込む。
    loadState() {
        try {
            const saved = localStorage.getItem(this.storageKey);
            if (!saved) return null;

            const parsed = JSON.parse(saved);
            if (!parsed || !parsed.sessionId) return null;

            const base = this.createInitialState();
            return {
                ...base,
                sessionId: parsed.sessionId,
                mode: parsed.mode || base.mode,
                audioFinalized: !!parsed.audioFinalized,
                audioUrl: parsed.audioUrl || null,
                audioFolderId: parsed.audioFolderId || null,
                mimeType: parsed.mimeType || null,
                expectedChunkCount: parsed.expectedChunkCount || 0
            };
        } catch (e) {
            console.warn("⚠️ 保存済みの状態を読み込めませんでした。初期化します。", e);
            return null;
        }
    }

    clearState() {
        try {
            localStorage.removeItem(this.storageKey);
        } catch (e) {
            console.warn("⚠️ 保存済みの状態の削除に失敗しました（無視して続行）:", e);
        }
        this.state = this.createInitialState();
        this.audioAppendFailures = [];
    }

    startNewSession() {
        this.state = this.createInitialState();
        this.isFinalizingAudio = false;
        this.audioAppendFailures = [];
        this.saveState();
        return this.state.sessionId;
    }

    // 🌟 テンプレート種別（templateType）をGAS側に渡す。GAS側ではセッションフォルダの
    //   説明文に保存され、バックグラウンド処理（processQueueTrigger）が
    //   議事録生成時に読み出して使用する。
    async appendAudioChunk(base64Data, mimeType, meetingName, chunkIndex, templateType) {
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
            chunkIndex: chunkIndex,
            templateType: templateType || "汎用議事録"
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

    // 🌟 録音完了をGASに通知する。GAS側で録音完了フラグが立ち、
    //   これ以降、バックグラウンド処理（processQueueTrigger）の対象になる。
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

                    // 🌟 サーバー側にセッションフォルダが見つからず audioFinalized:false が
                    //   返った場合は、確定済みとして状態を保存しない。
                    const isFinalized = response.audioFinalized !== false;

                    if (isFinalized) {
                        this.state.audioFinalized = true;
                        this.state.audioUrl = response.audioUrl || null;
                        this.state.audioFolderId = response.audioFolderId || null;
                        this.saveState();
                    }

                    console.log("✅ 音声データの確認が完了しました。", response);

                    return {
                        audioFinalized: isFinalized,
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

    // 🌟 GASへの通信の共通処理。実際の通信（fetch〜レスポンス解析）を
    //   requestQueue.enqueue() でラップしており、同時実行数の制限が自動的に効く。
    //   失敗時の詳細は DiagnosticsReporter.reportCommError() に記録される。
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

    // 🌟 お助けモード用：再開が必要な録音（1件以上のチャンクを送信済みの録音）があるか。
    hasPendingSession() {
        return !!(this.state && this.state.sessionId && this.state.expectedChunkCount > 0);
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
}

// 🚀 現場監督の出勤
window.appController = new RecorderController(CONFIG);
