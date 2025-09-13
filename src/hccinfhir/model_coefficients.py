from typing import Dict, Tuple, Optional
from hccinfhir.datamodels import ModelName, Demographics
from hccinfhir.database import get_db_session, RACoefficients

def load_coefficients_from_db(model_name: ModelName) -> Dict[Tuple[str, ModelName], float]:
    """Load coefficients from the database for a specific model."""
    db_session = get_db_session()
    try:
        model_domain, model_version_str = model_name.split(" Model ")
        model_version = model_version_str.split("V")[1]
        
        query = db_session.query(RACoefficients.coefficient, RACoefficients.value).filter(
            RACoefficients.model_domain == model_domain,
            RACoefficients.model_version.like(f'%{model_version}')
        )
        
        coefficients = {}
        for coefficient, value in query.all():
            key = (coefficient.lower(), model_name)
            coefficients[key] = float(value)
        return coefficients
    finally:
        db_session.close()

def get_coefficent_prefix(demographics: Demographics, 
                          model_name: ModelName = "CMS-HCC Model V28") -> str:

    """
    Get the coefficient prefix based on beneficiary demographics.
    
    Args:
        demographics: Demographics object containing beneficiary information
        
    Returns:
        String prefix used to look up coefficients for this beneficiary type
    """
    # Get base prefix based on model type
    if 'ESRD' in model_name:
        if demographics.esrd:
            if demographics.graft_months is not None:
                # Functioning graft case
                if demographics.lti:
                    return 'GI_'
                if demographics.new_enrollee:
                    return 'GNE_'
                    
                # Community functioning graft
                prefix = 'G'
                prefix += 'F' if demographics.fbd else 'NP'
                prefix += 'A' if demographics.age >= 65 else 'N'
                return prefix + '_'
                
            # Dialysis case
            return 'DNE_' if demographics.new_enrollee else 'DI_'
            
        # Transplant case
        if demographics.graft_months in [1, 2, 3]:
            return f'TRANSPLANT_KIDNEY_ONLY_{demographics.graft_months}M'
            
    elif 'RxHCC' in model_name:
        if demographics.lti:
            return 'Rx_NE_LTI_' if demographics.new_enrollee else 'Rx_CE_LTI_'
            
        if demographics.new_enrollee:
            return 'Rx_NE_Lo_' if demographics.low_income else 'Rx_NE_NoLo_'
            
        # Community case
        prefix = 'Rx_CE_'
        prefix += 'Low' if demographics.low_income else 'NoLow'
        prefix += 'Aged' if demographics.age >= 65 else 'NoAged'
        return prefix + '_'
        
    # Default CMS-HCC Model
    if demographics.lti:
        return 'INS_'
        
    if demographics.new_enrollee:

        return 'SNPNE_' if demographics.snp else 'NE_'
        
    # Community case
    prefix = 'C'
    prefix += 'F' if demographics.fbd else ('P' if demographics.pbd else 'N')
    prefix += 'A' if demographics.age >= 65 else 'D'
    return prefix + '_'


def apply_coefficients(demographics: Demographics, 
                      hcc_set: set[str], 
                      interactions: dict,
                      model_name: ModelName = "CMS-HCC Model V28",
                      coefficients: Optional[Dict[Tuple[str, ModelName], float]] = None) -> dict:
    """Apply risk adjustment coefficients to HCCs and interactions.

    This function takes demographic information, HCC codes, and interaction variables and returns
    a dictionary mapping each variable to its corresponding coefficient value based on the 
    specified model.

    Args:
        demographics: Demographics object containing patient characteristics
        hcc_set: Set of HCC codes present for the patient
        interactions: Dictionary of interaction variables and their values (0 or 1)
        model_name: Name of the risk adjustment model to use (default: "CMS-HCC Model V28")
        coefficients: Optional dictionary mapping (variable, model) tuples to coefficient values.
            If not provided, it will be loaded from the DB.

    Returns:
        Dictionary mapping HCC codes and interaction variables to their coefficient values
        for variables that are present (HCC in hcc_set or interaction value = 1)
    """
    if coefficients is None:
        coefficients = load_coefficients_from_db(model_name)

    # Get the coefficient prefix
    prefix = get_coefficent_prefix(demographics, model_name)
    
    output = {}

    demographics_key = (f"{prefix}{demographics.category}".lower(), model_name)
    if demographics_key in coefficients:
        output[demographics.category] = coefficients[demographics_key]

    # Apply the coefficients
    for hcc in hcc_set:
        key = (f"{prefix}HCC{hcc}".lower(), model_name)

        if key in coefficients:
            value = coefficients[key]
            output[hcc] = value

    # Add interactions
    for interaction_key, interaction_value in interactions.items():
        if interaction_value < 1:
            continue

        key = (f"{prefix}{interaction_key}".lower(), model_name)
        if key in coefficients:
            value = coefficients[key]
            output[interaction_key] = value


    return output