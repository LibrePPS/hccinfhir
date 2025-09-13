from typing import Dict, Set, Tuple, Optional
from hccinfhir.datamodels import ModelName
from hccinfhir.database import get_db_session, RAHierarchies

def load_hierarchies_from_db(model_name: ModelName) -> Dict[Tuple[str, ModelName], Set[str]]:
    """Load hierarchies from the database for a specific model."""
    db_session = get_db_session()
    try:
        query = db_session.query(RAHierarchies.cc_parent, RAHierarchies.cc_child).filter(RAHierarchies.model_fullname == model_name)
        hierarchies = {}
        for cc_parent, cc_child in query.all():
            key = (cc_parent, model_name)
            if key not in hierarchies:
                hierarchies[key] = set()
            hierarchies[key].add(cc_child)
        return hierarchies
    finally:
        db_session.close()

def apply_hierarchies(
    cc_set: Set[str],  # Set of active CCs
    model_name: ModelName = "CMS-HCC Model V28",
    hierarchies: Optional[Dict[Tuple[str, ModelName], Set[str]]] = None
) -> Set[str]:
    """
    Apply hierarchical rules to a set of CCs based on model version.

    Args:
        cc_set: Set of current active CCs
        model_name: HCC model name to use for hierarchy rules
        hierarchies: Optional custom hierarchy dictionary. If not provided, it will be loaded from the DB.
        
    Returns:
        Set of CCs after applying hierarchies
    """
    if hierarchies is None:
        hierarchies = load_hierarchies_from_db(model_name)

    # Track CCs that should be zeroed out
    to_remove = set()
    
    # For V28, if none of 221, 222, 224, 225, 226 are present, remove 223
    if model_name == "CMS-HCC Model V28":
        if ("223" in cc_set and 
            not any(cc in cc_set for cc in ["221", "222", "224", "225", "226"])):
            cc_set.remove("223")
    elif model_name == "CMS-HCC ESRD Model V21":
        if "134" in cc_set:
            cc_set.remove("134")
    elif model_name == "CMS-HCC ESRD Model V24":
        for cc in ["134", "135", "136", "137"]:
            if cc in cc_set:
                cc_set.remove(cc)

    # Apply hierarchies
    for cc in cc_set:
        hierarchy_key = (cc, model_name)
        if hierarchy_key in hierarchies:
            # If parent CC exists, remove all child CCs
            child_ccs = hierarchies[hierarchy_key]
            to_remove.update(child_ccs & cc_set)

    # Return CCs with hierarchical exclusions removed
    return cc_set - to_remove
