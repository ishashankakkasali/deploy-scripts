<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>CCE Insights — Sign in</title>
    <link rel="stylesheet" href="${url.resourcesPath}/css/login.css">
</head>
<body>
    <div class="gov-bar">
        <div class="gov-bar-left">
            <div class="gov-bar-icon">
                <img src="${url.resourcesPath}/img/rwanda-coat-of-arms.png" alt="Republic of Rwanda — Ministry of Health"/>
            </div>
            <div class="gov-bar-text">
                <span>Republic of Rwanda</span>
                <strong>Ministry of Health</strong>
            </div>
        </div>
        <div class="gov-bar-right">Care Coordination Engine</div>
    </div>

    <div class="page">
        <div class="card">

            <div class="banner">
                <img class="seal" src="${url.resourcesPath}/img/rwanda-coat-of-arms.png" alt="Republic of Rwanda — Ministry of Health"/>
                <h1>Care Coordination Engine</h1>
                <p>National care coordination and data insights</p>
            </div>

            <div class="body">
                <h2 class="subtitle">Sign in to continue</h2>

                <form action="${url.loginAction}" method="post">
                    <input type="hidden" name="credentialId" value="${(auth.selectedCredential!'')}"/>

                    <div class="field">
                        <label for="username">Username</label>
                        <input id="username" name="username" type="text"
                               autocomplete="username" autofocus
                               placeholder="cce.insights@moh.gov.rw"
                               value="${(login.username!'')}"/>
                    </div>

                    <div class="field">
                        <label for="password">Password</label>
                        <input id="password" name="password" type="password"
                               autocomplete="current-password"/>
                    </div>

                    <#if message?has_content && message.type == "error">
                        <div class="error-box">${message.summary}</div>
                    </#if>

                    <button type="submit">Sign In</button>
                </form>

                <p class="footer">Authorized MOH users only</p>
            </div>
        </div>
    </div>
</body>
</html>
