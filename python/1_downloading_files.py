import requests

urls = [
    f'https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2022_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260702T134502Z&X-Amz-SignedHeaders=host&X-Amz-Expires=604791&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV%2F20260702%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=a83be4ad5aeed33379ba83f7b6b462a0943c1c8625da977520baebc2dee25034',
    f'https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2022_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260702T134502Z&X-Amz-SignedHeaders=host&X-Amz-Expires=604791&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV%2F20260702%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=18bcf445f27816e6bbcbe98d4df1b5e8b4197652c43c12f5e9dd035d0367282c',
    f'https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2023_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260702T134502Z&X-Amz-SignedHeaders=host&X-Amz-Expires=604791&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV%2F20260702%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=9f57cfa02fa63696fbb7631c78731516c7e76ab771eb21b6c6a671ac09c33171',
    f'https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2023_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260702T134502Z&X-Amz-SignedHeaders=host&X-Amz-Expires=604791&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV%2F20260702%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=af76816c364ecb7a07fbc34d39377ae4907ab7be9c6080c66002e53487970f4a',
    f'https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv000.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260702T134502Z&X-Amz-SignedHeaders=host&X-Amz-Expires=604791&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV%2F20260702%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=c6229d2902dd63df9935d5a3e91be5809d8c75bd930df7b378e9c6c73097f252',
    f'https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv001.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260702T134502Z&X-Amz-SignedHeaders=host&X-Amz-Expires=604790&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV%2F20260702%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=c036c8522564967794cf0f92cc7fb2d0420c69efb21c291a58d4946adae710c0',
    f'https://1r-test-statsload.s3.dualstack.us-east-1.amazonaws.com/onerpm_data_analyst_test_data/onerpm_data_analyst_test_data_2024_csv002.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260702T134502Z&X-Amz-SignedHeaders=host&X-Amz-Expires=604790&X-Amz-Credential=AKIA3YUKOX75BLGJJRWV%2F20260702%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=92f6626c4318a34f731e27deac9af6ebac714222586a42b3cf1ebcc0ae52c64b'
]

file_names = [
    "2022_0.csv.gz",
    "2022_1.csv.gz",
    "2023_0.csv.gz",
    "2023_1.csv.gz"
    "2024_0.csv.gz",
    "2024_1.csv.gz",
    "2024_2.csv.gz"
]

for url, file_name in zip(urls, file_names):
    print(f"Downloading {file_name}...")

    response = requests.get(url)

    with open(file_name, "wb") as file:
        file.write(response.content)

print("Download completed.")