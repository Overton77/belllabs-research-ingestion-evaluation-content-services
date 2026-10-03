-- Add immutable scoped capability discovery custody; preserve every existing contract.
ALTER TABLE belllabs_control.immutable_documents DROP CONSTRAINT immutable_documents_contract_check;
ALTER TABLE belllabs_control.immutable_documents ADD CONSTRAINT immutable_documents_contract_check
 CHECK(contract IN ('goal.revision/1','goal.iteration/1','goal.handoff/1',
 'goal.verification/1','goal.template/1','stagegraph.template/1',
 'operation.binding/1','operation.binding-index/1','operation.settlement/1','operation.claim/1',
 'external-discovery-evidence/1','external-discovery-candidate/1',
 'external-inspection-workspace/1','external-inspection-report/1','external-inspection-workspace-report/1'));
