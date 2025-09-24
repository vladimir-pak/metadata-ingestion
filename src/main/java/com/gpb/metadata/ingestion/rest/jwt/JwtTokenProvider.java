package com.gpb.metadata.ingestion.rest.jwt;

import java.util.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class JwtTokenProvider {
    
    private final String jwtToken;
    
    public JwtTokenProvider(@Value("${jwt.token}") String jwtToken) {
        this.jwtToken = jwtToken;
    }
    
    public String getToken() {
        return jwtToken;
    }
    
    // Опционально: проверка валидности токена (без верификации подписи)
    public boolean isTokenValid() {
        try {
            // Проверяем, что токен может быть распарсен (базовая валидация)
            String[] parts = jwtToken.split("\\.");
            return parts.length == 3;
        } catch (Exception e) {
            return false;
        }
    }
    
    // Опционально: извлечение информации из токена
    public String getUsernameFromToken() {
        try {
            String[] parts = jwtToken.split("\\.");
            String payload = new String(Base64.getUrlDecoder().decode(parts[1]));
            JsonNode jsonNode = new ObjectMapper().readTree(payload);
            return jsonNode.has("sub") ? jsonNode.get("sub").asText() : null;
        } catch (Exception e) {
            return null;
        }
    }
}
