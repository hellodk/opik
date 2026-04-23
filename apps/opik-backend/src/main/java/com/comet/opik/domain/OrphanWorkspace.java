package com.comet.opik.domain;

import lombok.Builder;
import lombok.NonNull;

@Builder(toBuilder = true)
public record OrphanWorkspace(@NonNull String workspaceId, long orphanCount) {
}
