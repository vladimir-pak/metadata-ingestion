package com.gpb.metadata.ingestion.jwt;

import java.io.IOException;
import java.util.List;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import com.gpb.metadata.ingestion.cef.SvoiLogger;
import com.gpb.metadata.ingestion.cef.enums.SvoiSeverityEnum;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class JwtAuthFilter extends OncePerRequestFilter {

    private static final List<String> PROTECTED_PATHS =
            List.of(
                    "/api/v1/ingestion/**"
            );

    private static final AntPathMatcher PATH_MATCHER =
            new AntPathMatcher();

    private final JwtUtil jwtUtil;
    private final SvoiLogger svoiCustomLogger;

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain)
            throws ServletException, IOException {

        String path =
                getRequestPath(request);

        /*
         * JWT интересует только защищённый ingestion API.
         */
        if (!isProtectedPath(path)) {
            filterChain.doFilter(
                    request,
                    response
            );
            return;
        }

        String authorization =
                request.getHeader(
                        HttpHeaders.AUTHORIZATION
                );

        /*
         * ВАЖНО:
         *
         * Authorization отсутствует или это Basic.
         *
         * Ничего не отклоняем.
         * Дальше BasicAuthenticationFilter сам попробует
         * выполнить Basic authentication.
         */
        if (authorization == null
                || !authorization.startsWith("Bearer ")) {

            filterChain.doFilter(
                    request,
                    response
            );
            return;
        }

        String token =
                authorization.substring(7)
                        .trim();

        if (token.isBlank()) {
            unauthorized(
                    response,
                    "Bearer token is empty"
            );
            return;
        }

        String service =
                jwtUtil.validateAndGetService(
                        token
                );

        if (service == null) {
            logAuthFailed(
                    "Invalid or revoked token"
            );

            unauthorized(
                    response,
                    "Invalid token"
            );

            return;
        }

        /*
         * Не перезаписываем уже существующую authentication.
         */
        if (SecurityContextHolder
                .getContext()
                .getAuthentication() == null) {

            UsernamePasswordAuthenticationToken authentication =
                    new UsernamePasswordAuthenticationToken(
                            service,
                            null,
                            List.of()
                    );

            authentication.setDetails(
                    new WebAuthenticationDetailsSource()
                            .buildDetails(request)
            );

            SecurityContext context =
                    SecurityContextHolder
                            .createEmptyContext();

            context.setAuthentication(
                    authentication
            );

            SecurityContextHolder.setContext(
                    context
            );
        }

        filterChain.doFilter(
                request,
                response
        );
    }

    private boolean isProtectedPath(
            String path) {

        return PROTECTED_PATHS.stream()
                .anyMatch(
                        pattern ->
                                PATH_MATCHER.match(
                                        pattern,
                                        path
                                )
                );
    }

    private String getRequestPath(
            HttpServletRequest request) {

        String uri =
                request.getRequestURI();

        String contextPath =
                request.getContextPath();

        if (contextPath != null
                && !contextPath.isBlank()
                && uri.startsWith(contextPath)) {

            return uri.substring(
                    contextPath.length()
            );
        }

        return uri;
    }

    private void unauthorized(
            HttpServletResponse response,
            String message)
            throws IOException {

        response.setStatus(
                HttpStatus.UNAUTHORIZED.value()
        );

        response.setContentType(
                "application/json"
        );

        response.setCharacterEncoding(
                "UTF-8"
        );

        response.getWriter().write(
                """
                {"error":"%s"}
                """.formatted(message)
        );
    }

    private void logAuthFailed(
            String message) {

        svoiCustomLogger.sendInternal(
                "authFailed",
                "authFailed",
                "JWT auth failed. " + message,
                SvoiSeverityEnum.ONE
        );
    }
}