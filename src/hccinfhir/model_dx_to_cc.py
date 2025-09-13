from typing import List, Dict, Set, Tuple, Optional
from hccinfhir.datamodels import ModelName
from hccinfhir.database import get_db_session, RADxToCC

def load_dx_to_cc_mapping_from_db(model_name: ModelName) -> Dict[Tuple[str, ModelName], Set[str]]:
    """Load dx_to_cc mapping from the database for a specific model."""
    db_session = get_db_session()
    try:
        query = db_session.query(RADxToCC.diagnosis_code, RADxToCC.cc).filter(RADxToCC.model_name == model_name)
        mapping = {}
        for diagnosis_code, cc in query.all():
            key = (diagnosis_code, model_name)
            if key not in mapping:
                mapping[key] = set()
            mapping[key].add(cc)
        return mapping
    finally:
        db_session.close()

def get_cc(
    diagnosis_code: str,
    model_name: ModelName = "CMS-HCC Model V28",
    dx_to_cc_mapping: Optional[Dict[Tuple[str, ModelName], Set[str]]] = None
) -> Optional[Set[str]]:
    """
    Get CC for a single diagnosis code.

    Args:
        diagnosis_code: ICD-10 diagnosis code
        model_name: HCC model name to use for mapping
        dx_to_cc_mapping: Optional custom mapping dictionary. If not provided, it will be loaded from the DB.

    Returns:
        CC code if found, None otherwise
    """
    if dx_to_cc_mapping is None:
        dx_to_cc_mapping = load_dx_to_cc_mapping_from_db(model_name)

    return dx_to_cc_mapping.get((diagnosis_code, model_name))

def apply_mapping(
    diagnoses: List[str],
    model_name: ModelName = "CMS-HCC Model V28", 
    dx_to_cc_mapping: Optional[Dict[Tuple[str, ModelName], Set[str]]] = None
) -> Dict[str, Set[str]]:
    """
    Apply ICD-10 to CC mapping for a list of diagnosis codes.
    
    Args:
        diagnoses: List of ICD-10 diagnosis codes
        model_name: HCC model name to use for mapping
        dx_to_cc_mapping: Optional custom mapping dictionary. If not provided, it will be loaded from the DB.
        
    Returns:
        Dictionary mapping CCs to lists of diagnosis codes that map to them
    """
    if dx_to_cc_mapping is None:
        dx_to_cc_mapping = load_dx_to_cc_mapping_from_db(model_name)

    cc_to_dx: Dict[str, Set[str]] = {}
    
    for dx in set(diagnoses):
        dx = dx.upper().replace('.', '')
        ccs = get_cc(dx, model_name, dx_to_cc_mapping)
        if ccs is not None:
            for cc in ccs:
                if cc not in cc_to_dx:
                    cc_to_dx[cc] = set()
                cc_to_dx[cc].add(dx)
                
    return cc_to_dx