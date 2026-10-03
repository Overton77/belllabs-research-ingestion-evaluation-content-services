# Optional Biotech integration

This package contains application-specific schema grounding, reference research,
web research and Neo4j adapters extracted from the general runtime. Its import
namespace is biotech_mission_adapters. The general mission_control kernel must
never import this package; reviewed deployment composition supplies its policies,
extension validators and bounded capability bindings explicitly.

Retain application-specific tests and precise dependency requirements. Do not
manufacture permissive registrations or restore default Biotech branches in the
kernel. Extraction is not proof that these domain services are production-ready
or completion of the deferred KnowledgeServices generalization audit.

See repository docs/REMOVAL_GUIDE.md, docs/knowledge/architecture.md, and
docs/organization/biotech-module-map.json for source lineage.
