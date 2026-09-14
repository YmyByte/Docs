# SpringMVC
- [SpringMVC运行流程](#springmvc运行流程)
  - [SpringMVC的运行流程](#springmvc的运行流程)
  - [SpringBoot + Vue交互流程](#springboot-vue交互流程)
  - [HTTP 的 GET 和 POST 区别](#http的-get和-post区别)
  - [跨域请求是什么？有什么问题？怎么解决?](#跨域请求是什么？有什么问题？怎么解决)
  - [浏览器访问资源没有响应,怎么排查](#浏览器访问资源没有响应怎么排查)
  - [Cookie的理解](#cookie的理解)
  - [Session的理解](#session的理解)
    - [Cookie和Session的区别](#cookie和session的区别)

## SpringMVC的运行流程

1、**域名解析**。
2、**TCP三次握手建立连接**

3、客户端发送**http请求**，数据包通过网络传输到服务器指定的端口一般是80或者443，服务器监听改端口并且接受请求。

4、服务器接受请求后，首先将请求封装成为一个’**httpServletRequest**’对象，同时创建一个’**HttpServletResponse**’对象用于后续的响应。

5、请求分发到**DispatcherServlet**容器中，并根据handlermapping确定哪个控制器Controller应处理当前请求，通常是通过url路径映射到具体的控制器方法例如requestMapping注解就是用来配置url和方法的映射关系。

6、在controller之前，先会执行**handlerInterceptor拦截器**，这里可以进行一些例如权限校验、日志记录等信息，每个拦截器都可以决定是否继续处理请求或者直接返回。

7、Controller之后会执行业务层s**erver层具体业务逻辑**，这里如果使用到数据库还会调用dao层的一些方法，Controller方法一般会接收请求参数、执行业务逻辑，并最终返回一个ModelAndView对象。

8、**ModelAndView**对象包含了控制器返回的数据模型和视图的名称。模型部分通常是一个Map，包含了需要传递给视图的数据。

9、DispatcherServlet会将ModelAndView中的视图名称交给**ViewResolver解析**。ViewResolver根据配置找到实际的视图（例如JSP页面、Thymeleaf模板等）。

10、找到视图模板后，SpringMVC会将Controller方法返回的模型数据填充到视图中，最终生成用户能够看到的**HTML页面**。

11、生成的视图（通常是HTML页面）会通过DispatcherServlet返回给客户端，完成整个请求的处理流程。

12、请求结束后dispatchServlet执行清理工作：例如：清理上下文环境、销毁临时对象等。

总结来说，SpringMVC的请求处理流程从客户端请求开始，到DispatcherServlet的接收与分发，再到具体Controller的处理，最后通过ViewResolver解析视图并渲染，最终将响应返回给客户端。

## SpringBoot + Vue交互流程
1、前端请求：Vue通过使用`axios`来发起HTTP请求。常见的请求类型有GET、POST、PUT、DELETE等。前端请求可以是获取数据（GET请求）或者发送数据（POST请求等）。
2、后端处理：在SpringBoot中，后端的接口通常通过@RestController来定义。@RequestMapping、@GetMapping、@PostMapping等注解用于映射前端请求
3、返回数据：Spring Boot会返回数据，通常使用ResponseEntity或者直接返回对象。Vue接收到这些数据后，通常会在then回调中处理。
4、前后端协作：前后端通常使用JSON格式进行数据交互。Spring Boot通过@RequestBody注解自动将请求体中的JSON转换为Java对象，通过@ResponseBody将Java对象转换为JSON响应给前端。Vue则会通过axios自动处理这些JSON格式的数据。
5、CORS（跨域问题）在前后端分离的开发中，前后端通常部署在不同的端口或不同的服务器上，这可能导致跨域问题。

## HTTP 的 GET 和 POST 区别
| 特性           | GET                                   | POST                       |
| -------------- | ------------------------------------- | -------------------------- |
| **用途**       | 获取数据                              | 提交数据                   |
| **数据传输**   | 数据附加在 URL 后                     | 数据在请求体中             |
| **数据长度**   | 受 URL 长度限制（通常 2000 字符以内） | 没有严格限制               |
| **缓存**       | 可缓存                                | 不缓存                     |
| **历史记录**   | 被记录在浏览器历史中                  | 不会出现在浏览器历史中     |
| **数据可见性** | 数据暴露在 URL 中                     | 数据不暴露在 URL 中        |
| **安全性**     | 不适合传输敏感数据                    | 相对更安全，但仍需要 HTTPS |
| **幂等性**     | 幂等                                  | 非幂等                     |

> **幂等性**指的是同一个操作多次执行，结果和副作用相同。

## 跨域请求是什么？有什么问题？怎么解决?
跨域请求（同源策略）是指浏览器中，前端页面通过AJAX等方式向与当前页面不同的域（协议、域名、端口）发起的HTTP请求。目的：限制来自不同源（不同域、协议或端口）的脚本对网站的访问。
1、**产生原因**：
* 同源策略：浏览器只允许在同一个源（同协议、同域名、同端口）的情况下，页面和服务器进行通信。如果前端页面的域名与API服务器的域名不同，就会触发跨域请求。
* 不同的请求类型：一些请求（如GET、POST）一般不会引发跨域问题，但如果是需要身份验证的请求（如PUT、DELETE、PATCH）或者自定义的HTTP头，就可能会遇到跨域限制。
2、**跨域请求的问题**：
浏览器会根据同源策略阻止跨域请求，主要表现为：
* 请求被拦截：浏览器会阻止跨域请求的发起，并会报错，无法获取其他源的资源。
* 数据无法共享：由于跨域限制，前端无法访问不同域名或端口的数据，导致一些功能无法实现，如调用不同服务器的API。
3、**解决方法**：
* CORS（跨域资源共享）
CORS是浏览器和服务器之间的一种协议，它允许服务器通过设置HTTP头来告诉浏览器允许哪些跨域请求。服务器需要在响应头中加入适当的CORS信息，浏览器就能接受跨域请求。
常用CORS响应头：
    * Access-Control-Allow-Origin: 指定哪些源可以访问该资源。如果是*，表示允许任何源访问。
    * Access-Control-Allow-Methods: 指定允许的HTTP方法，如GET, POST, PUT等。
    * Access-Control-Allow-Headers: 允许的请求头，如Content-Type、Authorization等。
    * Access-Control-Allow-Credentials: 是否允许携带认证信息，如cookies。
* 代理服务器
    * 前端开发时，通常会在本地开发环境设置代理，代理会把请求转发到目标服务器上，避免跨域问题。
    Vue项目中可以配置`vue.config.js`文件：
    ```
    module.exports = {
      devServer: {
        proxy: {
          '/api': {
            target: 'http://localhost:8080',
            changeOrigin: true,  // 支持跨域
            pathRewrite: { '^/api': '' }
          }
        }
      }
    }
    ```
## 浏览器访问资源没有响应,怎么排查

1、**检查浏览器控制台和网络面板**，F12进入开发者模式，查看Console和NetWork是否有错误或失败的请求。

2、**检查网络连接**，确保 DNS 解析和网络连接正常。

3、**检查服务器状态**，确保服务器运行正常，并查看日志信息。

4、**检查 API 或后端服务**，确认服务是否正常。

5、**清除浏览器缓存或使用无痕模式**。

6、**检查跨域问题**，确保服务器配置了正确的 CORS 头。

7、**检查代理或 VPN 设置**，确保没有影响请求。

## Cookie的理解
Cookie：**服务器发送到用户浏览器并保存在本地的一小块数据**，他会在浏览器下次向同一服务器再发起请求时被携带并发送到服务器上。

应用场景：用户登陆状态、购物车、用户自定义设置、主题、跟踪分析用户行为等

## Session的理解
Session：服务器和客户端一次会话的过程
Session对象存储特定用户会话所需的属性及配置信息。用户在应用程序的web页之间相互跳转时，存储在Session对象中的变量将不会丢失，而是在整个用户会话中一直存在下去。

### Cookie和Session的区别
作用范围：Cookie保存在浏览器，Session保存在服务器端
存取方式：Cookie只能保存ASCII，SessionID，一般我们在Session中存储UserID、RoleID等常用变量信息
有效期：Cookie可以设置为长时间保存，比如我们默认使用的登录功能。Session一半失效时间比较短，客户端关闭或者Session超时都会失效。
安全性：Cookie存储在浏览器，会导致信息被窃取。Session存储在服务端，安全性相对于Cookie要好一些。
