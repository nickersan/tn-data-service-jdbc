package com.tn.service.data.jdbc.domain;

public record Column(String name, int type, boolean key, boolean nullable, boolean autoIncrement) {}