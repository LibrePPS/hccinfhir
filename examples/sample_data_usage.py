#!/usr/bin/env python3
"""
Example: Using Sample Data with HCCInFHIR

This script demonstrates how to use the sample data module to test and demonstrate
HCC calculations with pre-prepared sample data.
"""

from hccinfhir import (
    HCCInFHIR,
    get_eob_sample,
    get_eob_sample_list,
    get_837_sample,
    extract_sld,
    Demographics
)

def example_1_basic_eob_processing():
    """Example 1: Process a single EOB sample with HCC calculation."""
    print("=== Example 1: Basic EOB Processing ===")
    
    # Get sample EOB data
    eob_data = get_eob_sample(1)
    print(f"Retrieved EOB sample: {eob_data['resourceType']}")
    
    # Create sample demographics
    demographics = Demographics(
        age=65,
        sex="F",
        dual_elgbl_cd="00",
        orec="0",
        crec="0",
        new_enrollee=False,
        snp=False,
        low_income=False
    )
    
    # Process the EOB data
    processor = HCCInFHIR(rebuild_db=True)
    result = processor.run([eob_data], demographics)
    
    print(f"HCC Risk Score: {result.risk_score}")
    print(f"Diagnosis Codes: {result.diagnosis_codes}")
    print(f"Service Lines: {len(result.service_level_data)}")

    return result

def example_2_multiple_eob_samples():
    """Example 2: Process multiple EOB samples."""
    print("\n=== Example 2: Multiple EOB Samples ===")
    
    # Get multiple EOB samples (limited to 3 for demonstration)
    eob_list = get_eob_sample_list(limit=3)
    print(f"Processing {len(eob_list)} EOB samples")
    
    # Create demographics
    demographics = Demographics(
        age=70,
        sex="M",
        dual_elgbl_cd="00",
        orec="0",
        crec="0",
        new_enrollee=False,
        snp=False,
        low_income=False
    )
    
    # Process all samples
    processor = HCCInFHIR()
    result = processor.run(eob_list, demographics)
    
    print(f"Combined HCC Risk Score: {result.risk_score}")
    print(f"Total Diagnosis Codes: {len(result.diagnosis_codes)}")
    print(f"Total Service Lines: {len(result.service_level_data)}")

    return result

def example_3_837_claim_processing():
    """Example 3: Process 837 claim data."""
    print("\n=== Example 3: 837 Claim Processing ===")
    
    # Get sample 837 claim data
    claim_data = get_837_sample(0)
    print(f"Retrieved 837 claim: {len(claim_data)} characters")
    
    # Extract service level data from 837
    service_data = extract_sld(claim_data, format="837")
    print(f"Extracted {len(service_data)} service lines")
    
    # Show details of first few service lines
    for i, service in enumerate(service_data[:3]):
        print(f"  Service {i+1}:")
        print(f"    Procedure: {service.procedure_code}")
        print(f"    Diagnosis: {service.linked_diagnosis_codes}")
        print(f"    Date: {service.service_date}")
        print(f"    Provider: {service.performing_provider_npi}")
    
    return service_data

def example_4_compare_formats():
    """Example 4: Compare EOB vs 837 processing."""
    print("\n=== Example 4: Comparing EOB vs 837 Processing ===")
    
    # Get both types of sample data
    eob_data = get_eob_sample(1)
    claim_837 = get_837_sample(0)
    
    # Process EOB
    demographics = Demographics(
        age=65,
        sex="F",
        dual_elgbl_cd="00",
        orec="0",
        crec="0",
        new_enrollee=False,
        snp=False,
        low_income=False
    )
    
    processor = HCCInFHIR()
    eob_result = processor.run([eob_data], demographics)
    
    # Process 837
    service_data = extract_sld(claim_837, format="837")
    service_result = processor.run_from_service_data(service_data, demographics)
    
    print("EOB Processing:")
    print(f"  Risk Score: {eob_result.risk_score}")
    print(f"  Diagnosis Codes: {len(eob_result.diagnosis_codes)}")
    print(f"  Service Lines: {len(eob_result.service_level_data)}")

    print("\n837 Processing:")
    print(f"  Risk Score: {service_result.risk_score}")
    print(f"  Diagnosis Codes: {len(service_result.diagnosis_codes)}")
    print(f"  Service Lines: {len(service_result.service_level_data)}")
    
    return eob_result, service_result

def example_5_batch_processing():
    """Example 5: Batch processing with large sample set."""
    print("\n=== Example 5: Batch Processing ===")
    
    # Get a larger set of samples (limited to avoid memory issues in demo)
    eob_list = get_eob_sample_list(limit=10)
    print(f"Processing {len(eob_list)} EOB samples in batch")
    
    demographics = Demographics(
        age=68,
        sex="M",
        dual_elgbl_cd="00",
        orec="0",
        crec="0",
        new_enrollee=False,
        snp=False,
        low_income=False
    )
    
    processor = HCCInFHIR()
    result = processor.run(eob_list, demographics)
    
    print(f"Batch Processing Results:")
    print(f"  Total Risk Score: {result.risk_score}")
    print(f"  Unique Diagnosis Codes: {len(result.diagnosis_codes)}")
    print(f"  Total Service Lines: {len(result.service_level_data)}")

    # Show some sample diagnosis codes
    if result.diagnosis_codes:
        print(f"  Sample Diagnosis Codes: {result.diagnosis_codes[:5]}")
    
    return result

def main():
    """Run all examples."""
    print("HCCInFHIR Sample Data Usage Examples")
    print("=" * 60)
    
    try:
        # Run examples
        result1 = example_1_basic_eob_processing()
        result2 = example_2_multiple_eob_samples()
        service_data = example_3_837_claim_processing()
        eob_result, service_result = example_4_compare_formats()
        batch_result = example_5_batch_processing()
        
        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
