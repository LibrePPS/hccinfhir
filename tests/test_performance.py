import time
from hccinfhir import HCCInFHIR, Demographics
from hccinfhir.samples import get_eob_sample_list, get_demographics_sample

def test_claims_processing_performance():
    """Measures and reports the number of claims processed per second."""
    # Initialize the HCCInFHIR class
    hcc_processor = HCCInFHIR()

    # Load a sample of EOBs (Explanation of Benefits)
    # The get_eob_sample_list() function returns a list of 200 EOBs.
    # To test performance, we can increase this number by repeating the list.
    num_claims_to_process = 1000
    sample_eobs = get_eob_sample_list() # this is a list of 200
    eob_list = (sample_eobs * (num_claims_to_process // len(sample_eobs) + 1))[:num_claims_to_process]

    # Get sample demographics
    demographics = get_demographics_sample()

    # Measure the processing time
    start_time = time.time()
    hcc_processor.run(eob_list=eob_list, demographics=demographics)
    end_time = time.time()

    # Calculate and report performance
    processing_time = end_time - start_time
    claims_per_second = num_claims_to_process / processing_time

    print(f"\n--- Performance Test Results ---")
    print(f"Processed {num_claims_to_process} claims in {processing_time:.2f} seconds.")
    print(f"Claims per second: {claims_per_second:.2f}")
    print(f"--------------------------------")

    # Optional: Add an assertion for a minimum performance threshold
    # For example, assert claims_per_second > 100 # This is just an example threshold
