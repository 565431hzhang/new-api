package router

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/middleware"

	"github.com/gin-gonic/gin"
)

// SetRouter 注册所有路由，是路由注册的总入口。
//
// 路由注册顺序（顺序影响匹配优先级）：
//   1. SetApiRouter          — 管理后台 API（/api/*）
//   2. SetDashboardRouter    — 仪表盘相关路由
//   3. SetRelayRouter        — AI 模型转发路由（/v1/*, /mj/*, /v1beta/*）
//   4. SetTaskPluginProtocolRouter — 任务插件协议路由
//   5. SetVideoRouter        — 视频相关路由
//   6. SetTaskRouter         — 异步任务路由
//   7. SetPluginRouter       — 插件路由（返回分发器）
//   8. SetWebRouter / 重定向   — 前端静态资源或重定向到外部前端
//
// 前端部署模式：
//   - 内嵌模式（默认）：前端通过 embed.FS 嵌入二进制，由 SetWebRouter 服务静态文件
//   - 外部模式：配置 FRONTEND_BASE_URL 时，所有未匹配路由重定向到外部前端地址
//     （主节点忽略此配置，始终使用内嵌前端）
func SetRouter(router *gin.Engine, assets WebAssets) {
	SetApiRouter(router)
	SetDashboardRouter(router)
	SetRelayRouter(router)
	SetTaskPluginProtocolRouter(router)
	SetVideoRouter(router)
	SetTaskRouter(router)
	pluginDispatcher := SetPluginRouter(router)
	frontendBaseUrl := os.Getenv("FRONTEND_BASE_URL")
	if common.IsMasterNode && frontendBaseUrl != "" {
		frontendBaseUrl = ""
		common.SysLog("FRONTEND_BASE_URL is ignored on master node")
	}
	if frontendBaseUrl == "" {
		SetWebRouter(router, assets, pluginDispatcher)
	} else {
		frontendBaseUrl = strings.TrimSuffix(frontendBaseUrl, "/")
		router.NoRoute(
			pluginDispatcher,
			middleware.RouteTag("web"),
			middleware.AccessTokenAudit(),
			func(c *gin.Context) {
				c.Redirect(http.StatusMovedPermanently, fmt.Sprintf("%s%s", frontendBaseUrl, c.Request.RequestURI))
			},
		)
	}
}
