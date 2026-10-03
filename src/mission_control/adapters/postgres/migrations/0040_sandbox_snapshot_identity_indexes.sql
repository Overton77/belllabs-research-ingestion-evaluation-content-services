-- Preserve sandbox snapshot semantic identity and clone-target exclusivity per scope.
CREATE UNIQUE INDEX immutable_snapshot_creation_identity
    ON belllabs_control.immutable_documents(request_scope, (payload->>'creation_identity'))
    WHERE contract = 'sandbox.snapshot/1';
CREATE UNIQUE INDEX immutable_snapshot_clone_target
    ON belllabs_control.immutable_documents(
        request_scope, (payload->>'target_namespace_id'), (payload->>'target_workspace_id')
    ) WHERE contract = 'sandbox.snapshot.clone/1';
