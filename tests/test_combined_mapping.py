import pytest
import tempfile
import zipfile
import os
from hccinfhir.model_dx_to_cc import apply_mapping
from hccinfhir.model_hierarchies import apply_hierarchies
import importlib.resources
from typing import Dict, Set, Tuple
from hccinfhir.datamodels import ModelName

@pytest.fixture(scope="module")
def temp_data_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(os.path.dirname(__file__), "data", "hcc.sqlite")
        zip_path = os.path.join(os.path.dirname(importlib.resources.files('hccinfhir.data')), "data", "data.zip")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        yield temp_dir

def load_dx_to_cc_mapping(mapping_file: str, data_dir: str) -> Dict[Tuple[str, ModelName], Set[str]]:
    """Load dx to cc mapping from a CSV file."""
    dx_to_cc = {}
    try:
        with open(os.path.join(data_dir, mapping_file), 'r') as f:
            for line in f.readlines()[1:]:  # Skip header
                try:
                    dx, cc, model = line.strip().split(',')
                    key = (dx, model)
                    if key not in dx_to_cc:
                        dx_to_cc[key] = {cc}
                    else:
                        dx_to_cc[key].add(cc)
                except ValueError:
                    continue  # Skip malformed lines
    except Exception as e:
        print(f"Error loading mapping file: {e}")
        dx_to_cc = {}
    return dx_to_cc

def load_hierarchies(hierarchies_file: str, data_dir: str) -> Dict[Tuple[str, ModelName], Set[str]]:
    """Load hierarchies from a CSV file."""
    hierarchies = {}
    try:
        with open(os.path.join(data_dir, hierarchies_file), 'r') as f:
            for line in f.readlines()[1:]:  # Skip header
                try:
                    cc_parent, cc_child, model_domain, model_version, _ = line.strip().split(',')
                    if model_domain == 'ESRD':
                        model_name = f"CMS-HCC {model_domain} Model {model_version}"
                    else:
                        model_name = f"{model_domain} Model {model_version}"
                    key = (cc_parent, model_name)
                    if key not in hierarchies:
                        hierarchies[key] = {cc_child}
                    else:
                        hierarchies[key].add(cc_child)
                except ValueError:
                    continue  # Skip malformed lines
    except Exception as e:
        print(f"Error loading mapping file: {e}")
        hierarchies = {}
    return hierarchies

def test_diabetes_hierarchy_chain_with_custom_files(temp_data_dir):
    """Test complete chain from diagnosis codes to final hierarchical CCs for diabetes"""
    # Type 1 diabetes with complications maps to HCC 17
    # Type 2 diabetes with complications maps to HCC 18
    # Diabetes without complications maps to HCC 19
    diagnoses = [
        "E1100",  # type 2 diabetes mellitus with hyperosmolarity without nonketotic hyperglycemic-hyperosmolar coma (NKHHC)
        "E1022",  # Type 1 diabetes with kidney complications
        "E1165",  # Type 2 diabetes with circulatory complications
        "E119"    # Type 2 diabetes without complications
    ]
    dx_to_cc_mapping = load_dx_to_cc_mapping('ra_dx_to_cc_2025.csv', temp_data_dir)
    hierarchies = load_hierarchies('ra_hierarchies_2025.csv', temp_data_dir)
    # First map dx to CCs
    cc_to_dx = apply_mapping(diagnoses, 
                             model_name="CMS-HCC Model V24",
                             dx_to_cc_mapping=dx_to_cc_mapping)
    ccs = set(cc_to_dx.keys())

    # Then apply hierarchies
    final_ccs = apply_hierarchies(ccs, 
                                  model_name="CMS-HCC Model V24",
                                  hierarchies=hierarchies)

    # Should only keep the highest severity CC (17)
    assert final_ccs == {"17"}

    dx_to_cc_mapping = load_dx_to_cc_mapping('ra_dx_to_cc_2026.csv', temp_data_dir)
    hierarchies = load_hierarchies('ra_hierarchies_2026.csv', temp_data_dir)

    cc_to_dx = apply_mapping(diagnoses, 
                             model_name="CMS-HCC Model V28",
                             dx_to_cc_mapping=dx_to_cc_mapping)
    ccs = set(cc_to_dx.keys())
    final_ccs = apply_hierarchies(ccs, 
                                  model_name="CMS-HCC Model V28",
                                  hierarchies=hierarchies)
    assert final_ccs == {"36"}