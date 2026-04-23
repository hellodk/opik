package com.comet.opik.infrastructure;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import io.dropwizard.util.Duration;
import io.dropwizard.validation.MaxDuration;
import io.dropwizard.validation.MinDuration;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Data
public class ExperimentProjectMigrationConfig {

    @JsonProperty
    @NotNull private boolean enabled;

    @JsonProperty
    @NotNull @Min(1) @Max(100) private int workspacesPerRun;

    @JsonProperty
    @NotNull @Min(1) @Max(10000) private int experimentBatchSize;

    @JsonProperty
    @NotNull @MinDuration(value = 5, unit = TimeUnit.SECONDS)
    @MaxDuration(value = 1, unit = TimeUnit.HOURS)
    private Duration interval;

    @JsonProperty
    @NotNull @MinDuration(value = 0, unit = TimeUnit.SECONDS)
    @MaxDuration(value = 1, unit = TimeUnit.HOURS)
    private Duration startupDelay;

    @JsonProperty
    @NotNull @MinDuration(value = 1, unit = TimeUnit.SECONDS)
    @MaxDuration(value = 2, unit = TimeUnit.HOURS)
    private Duration lockTimeout;

    @JsonProperty
    @NotNull @MinDuration(value = 1, unit = TimeUnit.SECONDS)
    @MaxDuration(value = 2, unit = TimeUnit.HOURS)
    private Duration jobTimeout;

    @NotNull private List<String> excludedWorkspaceIds = List.of();

    @JsonSetter
    public void setExcludedWorkspaceIds(String commaSeparated) {
        this.excludedWorkspaceIds = Optional.ofNullable(commaSeparated)
                .filter(StringUtils::isNotBlank)
                .map(s -> Arrays.stream(s.split(","))
                        .map(String::strip)
                        .filter(StringUtils::isNotBlank)
                        .toList())
                .orElse(List.of());
    }
}
