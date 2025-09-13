from typing import List, Set, Optional
from hccinfhir.datamodels import ServiceLevelData
from hccinfhir.database import get_db_session, RAEligibleCptHcpcs

def load_proc_filtering_from_db(year: int) -> Set[str]:
    """Load professional CPT/HCPCS codes from the database for a specific year."""
    db_session = get_db_session()
    try:
        query = db_session.query(RAEligibleCptHcpcs.cpt_hcpcs_code).filter(RAEligibleCptHcpcs.year == year)
        return {row[0] for row in query.all()}
    finally:
        db_session.close()

def apply_filter(
    data: List[ServiceLevelData], 
    inpatient_tob: Set[str] = {'11X', '41X'},
    outpatient_tob: Set[str] = {'12X', '13X', '43X', '71X', '73X', '76X', '77X', '85X', '87X'},
    professional_cpt: Optional[Set[str]] = None,
    year: int = 2025
) -> List[ServiceLevelData]:
    # tob (Type of Bill) Filter is based on:
    # https://www.hhs.gov/guidance/sites/default/files/hhs-guidance-documents/2012181486-wq-092916_ra_webinar_slides_5cr_092816.pdf
    # https://www.hhs.gov/guidance/sites/default/files/hhs-guidance-documents/FinalEncounterDataDiagnosisFilteringLogic.pdf
    # https://www.cms.gov/files/document/encounterdatasystemedit20495andedit01415andtob87x07162021.pdf for 87X
    # NOTE: If no facility_type or service_type, then the claim is professional, in our implementation.
    # NOTE: The original CMS logic is for the "record" level, not the service level.
    #  Thus, when preparing the service level data, put all diagnosis codes into the diagnosis field.

    if professional_cpt is None:
        professional_cpt = load_proc_filtering_from_db(year)

    filtered_data = []
    for item in data:
        item_tob = '?' if item.facility_type is None else item.facility_type 
        item_tob += '?' if item.service_type is None else item.service_type 
        item_tob += 'X'

        if '?' in item_tob: # professional claims
            if item.procedure_code in professional_cpt:
                filtered_data.append(item)
        else:
            if item_tob in inpatient_tob:
                filtered_data.append(item)
            elif item_tob in outpatient_tob:
                if item.procedure_code in professional_cpt:
                    filtered_data.append(item)
            
    return filtered_data
