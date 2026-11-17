/**
 * レコーダーアプリ ミドルエンド（現場監督）
 * 役割：録音データの管理、進捗の記憶、GASへの送信（同時実行数の制限＋自動リトライ）、音声の確定保存
 */

const CONFIG = {
    GAS_URL: localStorage.getItem('saved_gas_url') || "",
    AUTH_TOKEN: localStorage.getItem('saved_auth_token') || "",
    MAX_RETRIES: 3,
    STORAGE_KEY: "recorder_app_state",
    MAX_CONCURRENT_REQUESTS: 2
};

// ==========================================
// 🌟 GASへの同時リクエスト数を制限するキュー
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
// 🌟 診断情報の保持担当
// ==========================================
const DiagnosticsReporter = {
    reports: [],

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

    clearReports: function() {
        this.reports = [];
        window.lastDiagnosticsText = "";
        const textarea = document.getElementById('diagnosticsText');
        if (textarea) {
            textarea.value = "";
        }
    },

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
        this._openPanel();
    }
};

class RecorderController {
    constructor(config) {
        this.gasUrl = config.GAS_URL;
        this.authToken = config.AUTH_TOKEN || "";
        this.maxRetries = config.MAX_RETRIES;
        this.storageKey = config.STORAGE_KEY;
        this.state = this.loadState() || this.createInitialState();

        this.isFinalizingAudio = false;
        this.audioAppendFailures = [];
        this.requestQueue = new RequestQueue(config.MAX_CONCURRENT_REQUESTS || 2);
    }

    createInitialState() {
        return {
            sessionId: Date.now().toString(),
            mode: '3',
            audioFinalized: false,
            pendingFinalize: false,
            audioUrl: null,
            audioFolderId: null,
            mimeType: null,
            meetingName: "無題の会議",
            templateType: "汎用議事録",
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
                pendingFinalize: !!parsed.pendingFinalize,
                audioUrl: parsed.audioUrl || null,
                audioFolderId: parsed.audioFolderId || null,
                mimeType: parsed.mimeType || null,
                meetingName: parsed.meetingName || base.meetingName,
                templateType: parsed.templateType || base.templateType,
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

    markPendingFinalize(meetingName, templateType) {
        if (meetingName) this.state.meetingName = meetingName;
        if (templateType) this.state.templateType = templateType;
        this.state.pendingFinalize = true;
        this.saveState();
    }

    async appendAudioChunk(base64Data, mimeType, meetingName, chunkIndex, templateType) {
        if (!this.state.mimeType) {
            this.state.mimeType = mimeType || 'audio/webm';
        }
        if (meetingName) this.state.meetingName = meetingName;
        if (templateType) this.state.templateType = templateType;

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

    async finalizeAudioAndGetResult(meetingName, templateType, recordingNotes) {
        if (this.state.audioFinalized) {
            return {
                audioFinalized: true,
                audioUrl: this.state.audioUrl,
                audioFolderId: this.state.audioFolderId,
                alreadyFinalized: true
            };
        }

        if (this.isFinalizingAudio) {
            return null;
        }
        this.isFinalizingAudio = true;

        const finalMeetingName = meetingName || this.state.meetingName || "無題の会議";
        const finalTemplateType = templateType || this.state.templateType || "汎用議事録";
        this.markPendingFinalize(finalMeetingName, finalTemplateType);

        const payload = {
            action: "finalizeAudio",
            sessionId: this.state.sessionId,
            meetingName: finalMeetingName,
            mimeType: this.state.mimeType || 'audio/webm',
            templateType: finalTemplateType
        };
        if (recordingNotes) {
            payload.recordingNotes = recordingNotes;
        }

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
                    const isFinalized = response.audioFinalized !== false;

                    if (isFinalized) {
                        this.state.audioFinalized = true;
                        this.state.pendingFinalize = false;
                        this.state.audioUrl = response.audioUrl || null;
                        this.state.audioFolderId = response.audioFolderId || null;
                        this.saveState();
                    }

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
                "「レコーダー/音声データ」フォルダ内に残っている可能性があります。詳細: " +
                (lastError ? lastError.message : "不明なエラー")
            );

        } finally {
            this.isFinalizingAudio = false;
        }
    }

    async callGasApi(payload) {
        if (!this.gasUrl) throw new Error("GASのURLが設定されていません。");

        const bodyPayload = this.authToken
            ? { ...payload, authToken: this.authToken }
            : payload;

        return this.requestQueue.enqueue(async () => {
            const response = await fetch(this.gasUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'text/plain' },
                body: JSON.stringify(bodyPayload)
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
        return !!(this.state && this.state.sessionId && this.state.expectedChunkCount > 0);
    }

    async checkSessionStatus() {
        if (!this.state.sessionId) {
            return { status: "not_found" };
        }
        return await this.callGasApi({
            action: "checkSessionStatus",
            sessionId: this.state.sessionId
        });
    }
}

window.appController = new RecorderController(CONFIG);
