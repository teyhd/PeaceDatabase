package io.peacedb.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Document {
    @JsonProperty("id")
    public String id;
    @JsonProperty("rev")
    public String rev;
    @JsonProperty("deleted")
    public boolean deleted;
    @JsonProperty("data")
    public Map<String, Object> data;
    @JsonProperty("tags")
    public List<String> tags;
    @JsonProperty("content")
    public String content;
}
