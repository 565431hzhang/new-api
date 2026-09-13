// Package channel 定义了 new-api 的渠道适配器（Adaptor）抽象层。
//
// new-api 是一个 AI 模型聚合网关，它将不同厂商的 API（OpenAI、Claude、Gemini、
// 百度、阿里、讯飞等）统一为一致的接口。每个厂商对应一个 Adaptor 实现，
// 负责请求转换、签名鉴权、响应解析等差异化逻辑。
//
// 适配器分为两大类：
//   - Adaptor：处理同步请求（聊天补全、嵌入、图像生成、音频、重排序等），
//     请求-响应在同一次 HTTP 调用中完成。
//   - TaskAdaptor：处理异步任务（Midjourney、Suno、视频生成等），
//     需要提交任务后轮询状态，最终获取结果。支持分阶段计费。
package channel

import (
	"io"
	"net/http"

	taskdto "github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	hosttypes "github.com/QuantumNous/new-api/types"

	"github.com/gin-gonic/gin"
)

// Adaptor 是同步渠道适配器的核心接口。
// 每种 AI 厂商（OpenAI、Claude、Gemini、百度等）实现此接口，
// 将统一的 OpenAI 格式请求转换为对应厂商的专有格式。
//
// 典型请求生命周期：
//  1. Init       — 初始化适配器，设置是否流式等参数
//  2. ConvertXxx — 将统一格式请求转换为厂商专有格式
//  3. GetRequestURL + SetupRequestHeader — 构建上游请求的 URL 和 Headers
//  4. DoRequest  — 发送请求到上游
//  5. DoResponse — 解析上游响应，写入客户端响应并返回用量信息
type Adaptor interface {
	// Init 根据请求信息初始化适配器状态，包括是否流式响应等。
	Init(info *relaycommon.RelayInfo)
	// GetRequestURL 返回该适配器对应厂商的上游请求 URL。
	GetRequestURL(info *relaycommon.RelayInfo) (string, error)
	// SetupRequestHeader 设置发往上游的 HTTP 请求头（鉴权、Content-Type 等）。
	SetupRequestHeader(c *gin.Context, req *http.Header, info *relaycommon.RelayInfo) error
	// ConvertOpenAIRequest 将 OpenAI Chat Completions 格式请求转换为厂商专有格式。
	ConvertOpenAIRequest(c *gin.Context, info *relaycommon.RelayInfo, request *dto.GeneralOpenAIRequest) (any, error)
	// ConvertRerankRequest 将重排序（Rerank）请求转换为厂商专有格式。
	ConvertRerankRequest(c *gin.Context, relayMode int, request dto.RerankRequest) (any, error)
	// ConvertEmbeddingRequest 将嵌入（Embedding）请求转换为厂商专有格式。
	ConvertEmbeddingRequest(c *gin.Context, info *relaycommon.RelayInfo, request dto.EmbeddingRequest) (any, error)
	// ConvertAudioRequest 将音频（语音转文字/翻译/合成）请求转换为厂商专有格式。
	ConvertAudioRequest(c *gin.Context, info *relaycommon.RelayInfo, request dto.AudioRequest) (io.Reader, error)
	// ConvertImageRequest 将图像生成/编辑请求转换为厂商专有格式。
	ConvertImageRequest(c *gin.Context, info *relaycommon.RelayInfo, request dto.ImageRequest) (any, error)
	// ConvertOpenAIResponsesRequest 将 OpenAI Responses API 请求转换为厂商专有格式。
	ConvertOpenAIResponsesRequest(c *gin.Context, info *relaycommon.RelayInfo, request dto.OpenAIResponsesRequest) (any, error)
	// DoRequest 使用构建好的请求体发送 HTTP 请求到上游服务。
	DoRequest(c *gin.Context, info *relaycommon.RelayInfo, requestBody io.Reader) (any, error)
	// DoResponse 解析上游 HTTP 响应，写入客户端响应，并返回用量（token 数）信息。
	DoResponse(c *gin.Context, resp *http.Response, info *relaycommon.RelayInfo) (usage any, err *types.NewAPIError)
	// GetModelList 返回该渠道支持的所有模型名称列表。
	GetModelList() []string
	// GetChannelName 返回该渠道的人类可读名称。
	GetChannelName() string
	// ConvertClaudeRequest 将 Claude/Anthropic Messages 格式请求转换为厂商专有格式。
	ConvertClaudeRequest(c *gin.Context, info *relaycommon.RelayInfo, request *dto.ClaudeRequest) (any, error)
	// ConvertGeminiRequest 将 Gemini 格式请求转换为厂商专有格式。
	ConvertGeminiRequest(c *gin.Context, info *relaycommon.RelayInfo, request *dto.GeminiChatRequest) (any, error)
}

// TaskAdaptor 是异步任务适配器接口，用于处理需要轮询的 AI 任务
// （如 Midjourney 图像生成、Suno 音乐生成、视频生成等）。
//
// 与 Adaptor 不同，任务适配器处理的是"提交→轮询→完成"的异步流程：
//  1. Init                      — 初始化适配器
//  2. ValidateRequestAndSetAction — 验证请求并确定任务动作
//  3. EstimateBilling            — 根据请求参数预估费用并预扣费
//  4. BuildRequestURL/Header/Body — 构建上游提交请求
//  5. DoRequest + ParseResponse  — 提交任务到上游，解析任务 ID
//  6. 轮询阶段：FetchTask → ParseTaskResult → AdjustBillingOnComplete
//  7. AdjustBillingOnComplete     — 任务完成时根据实际用量结算费用差额
//
// 计费采用三阶段模型：预估预扣 → 提交时调整 → 完成时结算，
// 确保用户不会多付也不会少付。
type TaskAdaptor interface {
	// Init 根据请求信息初始化任务适配器状态。
	Init(info *relaycommon.RelayInfo)

	// ValidateRequestAndSetAction 验证客户端请求的合法性，并从请求中
	// 解析出任务动作（如 imagine、variation、upscale 等），设置到 RelayInfo 中。
	ValidateRequestAndSetAction(c *gin.Context, info *relaycommon.RelayInfo) *taskdto.TaskError

	// ── Billing（计费） ─────────────────────────────────────────────

	// EstimateBilling 返回基于用户请求参数的 OtherRatios（额外计费比率），
	// 用于预扣费。在 ValidateRequestAndSetAction 之后、价格计算之前调用。
	// 适配器应从解析后的请求中提取时长、分辨率等参数，
	// 作为比率乘数返回（例如 {"seconds": 5, "size": 1.666}）。
	// 返回 nil 表示使用模型基础价格，不加额外比率。
	EstimateBilling(c *gin.Context, info *relaycommon.RelayInfo) map[string]float64

	// AdjustBillingOnSubmit 根据上游提交响应中的实际参数返回调整后的 OtherRatios。
	// 在 ParseResponse 成功后调用。如果上游返回的实际参数与预估值不同
	// （例如实际生成秒数），返回更新后的比率以便调用方重新计算配额，
	// 并与预扣费金额结算差额。返回 nil 表示无需调整。
	AdjustBillingOnSubmit(info *relaycommon.RelayInfo, taskData []byte) map[string]float64

	// AdjustBillingOnComplete 在任务轮询到达终态（成功/失败）时返回实际配额。
	// 由轮询循环在 ParseTaskResult 之后调用。
	// 返回正值触发差额结算（补扣或退还）。返回 0 表示保持预扣金额不变。
	AdjustBillingOnComplete(task *model.Task, taskResult *relaycommon.TaskInfo) int

	// ── 请求/响应（Request/Response） ───────────────────────────────

	// BuildRequestURL 构建上游任务提交 URL。
	BuildRequestURL(info *relaycommon.RelayInfo) (string, error)
	// BuildRequestHeader 设置上游请求的 HTTP 头（鉴权等）。
	BuildRequestHeader(c *gin.Context, req *http.Request, info *relaycommon.RelayInfo) error
	// BuildRequestBody 构建上游任务提交的请求体。
	BuildRequestBody(c *gin.Context, info *relaycommon.RelayInfo) (io.Reader, error)

	// DoRequest 发送任务提交请求到上游服务。
	DoRequest(c *gin.Context, info *relaycommon.RelayInfo, requestBody io.Reader) (*http.Response, error)
	// ParseResponse 解析上游任务提交响应，提取任务 ID 和数据。
	// 注意：解析阶段不应直接写入客户端响应。
	ParseResponse(c *gin.Context, resp *http.Response, info *relaycommon.RelayInfo) (*TaskSubmitResponse, *taskdto.TaskError)

	GetModelList() []string
	GetChannelName() string

	// ── 轮询（Polling） ─────────────────────────────────────────────

	// FetchTask 向上游发起查询请求，获取指定任务的当前状态。
	FetchTask(baseUrl, key string, task *model.Task, proxy string) (*http.Response, error)
	// ParseTaskResult 解析上游任务查询响应，提取任务状态、进度和结果。
	ParseTaskResult(task *model.Task, resp *http.Response, respBody []byte) (*relaycommon.TaskInfo, error)
}

// TaskSubmitResponse is the transport-independent result of parsing an
// upstream task submission. Parsing must not write to the client response.
type TaskSubmitResponse struct {
	UpstreamTaskID string
	TaskData       []byte
	ClientResponse any
	Immediate      *relaycommon.TaskInfo
	PluginState    []byte
}

type OpenAIVideoConverter interface {
	ConvertToOpenAIVideo(originTask *model.Task) ([]byte, error)
}

type TaskArtifact = hosttypes.TaskArtifact

type TaskArtifactClientRequest struct {
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers,omitempty"`
}

type TaskArtifactProvider interface {
	ListArtifacts(task *model.Task) ([]TaskArtifact, error)
}

type TaskContentRequest struct {
	URL            string
	Method         string
	Headers        map[string]string
	Body           []byte
	Credentialless bool
}

type TaskContentRequestProvider interface {
	BuildContentRequest(task *model.Task, artifactKey string, clientRequest TaskArtifactClientRequest) (*TaskContentRequest, error)
}

type TaskUsageFactsProvider interface {
	ExtractUsageFacts(c *gin.Context, info *relaycommon.RelayInfo) map[string]any
}

// TaskValidatedBillingProvider lets an adaptor reject invalid usage facts at
// the existing estimate point, after model mapping and before quota
// multiplication. Non-plugin task adaptors keep using EstimateBilling.
type TaskValidatedBillingProvider interface {
	EstimateBillingValidated(c *gin.Context, info *relaycommon.RelayInfo) (map[string]float64, error)
}

// TaskValidatedUsageFactsProvider is the tiered-billing counterpart to
// TaskValidatedBillingProvider.
type TaskValidatedUsageFactsProvider interface {
	ExtractUsageFactsValidated(c *gin.Context, info *relaycommon.RelayInfo) (map[string]any, error)
}
