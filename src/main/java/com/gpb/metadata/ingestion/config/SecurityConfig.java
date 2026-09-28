package com.gpb.metadata.ingestion.config;

import com.gpb.metadata.ingestion.jwt.JwtAuthFilter;
import com.gpb.metadata.ingestion.service.CustomAuthenticationEntryPoint;

import lombok.RequiredArgsConstructor;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;

import org.springframework.security.config.http.SessionCreationPolicy;

import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

@Configuration
@RequiredArgsConstructor
public class SecurityConfig {

    private final UserDetailsService userDetailsService;

    private final CustomAuthenticationEntryPoint
            customAuthenticationEntryPoint;

    private final JwtAuthFilter
            jwtAuthFilter;

    @Bean
    public SecurityFilterChain securityFilterChain(
            HttpSecurity http)
            throws Exception {

        http
                .csrf(
                        AbstractHttpConfigurer::disable
                )

                /*
                 * Для REST API лучше не хранить authentication
                 * между запросами в HTTP session.
                 */
                .sessionManagement(
                        session ->
                                session.sessionCreationPolicy(
                                        SessionCreationPolicy.STATELESS
                                )
                )

                .authorizeHttpRequests(
                        auth -> auth

                                /*
                                 * Этот endpoint может быть
                                 * аутентифицирован:
                                 *
                                 * Basic OR JWT Bearer.
                                 */
                                .requestMatchers(
                                        "/api/v1/ingestion/**"
                                )
                                .authenticated()

                                .anyRequest()
                                .permitAll()
                )

                /*
                 * BASIC
                 */
                .httpBasic(
                        basic ->
                                basic.authenticationEntryPoint(
                                        customAuthenticationEntryPoint
                                )
                )

                /*
                 * JWT должен идти ДО BasicAuthenticationFilter.
                 */
                .addFilterBefore(
                        jwtAuthFilter,
                        BasicAuthenticationFilter.class
                )

                .exceptionHandling(
                        exception ->
                                exception.authenticationEntryPoint(
                                        customAuthenticationEntryPoint
                                )
                );

        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public DaoAuthenticationProvider
            authenticationProvider() {

        DaoAuthenticationProvider provider =
                new DaoAuthenticationProvider();

        provider.setUserDetailsService(
                userDetailsService
        );

        provider.setPasswordEncoder(
                passwordEncoder()
        );

        return provider;
    }

    /*
     * JwtAuthFilter является Spring bean (@Component).
     *
     * Не позволяем Spring Boot отдельно зарегистрировать
     * его как обычный servlet Filter.
     *
     * Он должен жить ТОЛЬКО внутри SecurityFilterChain.
     */
    @Bean
    public FilterRegistrationBean<JwtAuthFilter>
            jwtFilterRegistration(
                    JwtAuthFilter filter) {

        FilterRegistrationBean<JwtAuthFilter>
                registration =
                new FilterRegistrationBean<>(
                        filter
                );

        registration.setEnabled(false);

        return registration;
    }
}