package com.gpb.metadata.ingestion.model;

import java.io.Serial;
import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter 
@EqualsAndHashCode 
@ToString 
@NoArgsConstructor
@AllArgsConstructor
public class EntityId implements Serializable {

    @Serial 
    private static final long serialVersionUID = 1L;

    private Long id;

    private String parentFqn;
}
