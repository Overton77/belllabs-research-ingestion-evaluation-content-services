from __future__ import annotations

from app.domain.schema_catalog.errors import CatalogValidationError
from app.domain.schema_catalog.models import (
    PhysicalSchemaCatalog,
    SemanticOverlay,
    ValidationIssue,
)


def validate_catalog_overlay(
    catalog: PhysicalSchemaCatalog, overlay: SemanticOverlay
) -> tuple[ValidationIssue, ...]:
    issues: list[ValidationIssue] = []
    valid_elements = catalog.element_ids()
    module_names = [module.name for module in overlay.modules]
    known_modules = set(module_names)
    if len(known_modules) != len(module_names):
        issues.append(
            ValidationIssue(code="duplicate-module", message="module names must be unique")
        )

    for module in overlay.modules:
        for element_id in module.seed_elements:
            if element_id not in valid_elements:
                issues.append(
                    ValidationIssue(
                        code="unknown-module-seed",
                        message=f"module {module.name!r} references an unknown seed",
                        element_id=element_id,
                    )
                )
    for element_id, semantics in overlay.elements.items():
        if element_id not in valid_elements:
            issues.append(
                ValidationIssue(
                    code="unknown-element",
                    message="semantic metadata references an element absent from authoritative SDL",
                    element_id=element_id,
                )
            )
        for module_name in semantics.modules:
            if module_name not in known_modules:
                issues.append(
                    ValidationIssue(
                        code="unknown-module",
                        message=f"element references unknown module {module_name!r}",
                        element_id=element_id,
                    )
                )
        if semantics.maturity == "deprecated" and semantics.replacement_element:
            if semantics.replacement_element not in valid_elements:
                issues.append(
                    ValidationIssue(
                        code="unknown-replacement",
                        message="deprecated element replacement is absent from authoritative SDL",
                        element_id=element_id,
                    )
                )
    return tuple(issues)


def require_valid_catalog_overlay(catalog: PhysicalSchemaCatalog, overlay: SemanticOverlay) -> None:
    issues = validate_catalog_overlay(catalog, overlay)
    if issues:
        summary = "; ".join(f"{issue.code}:{issue.element_id or '-'}" for issue in issues[:10])
        raise CatalogValidationError(f"semantic overlay is incompatible with SDL: {summary}")
