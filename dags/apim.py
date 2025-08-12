from typing import Generator
from airflow.sdk import asset, dag, Metadata, Context, ObjectStoragePath, Connection



@asset(
    name="apim_apis",
    description="This asset is responsible for acquiring data from the API and storing it in the Azure Blob Storage.",
    tags=["apim"],
    schedule="@daily",
)
def apim_apis_asset(context: Context) -> Generator[Metadata, None, None]:
    from projects.apim.data_fetch import fetch_apis
    from projects.core.blob_manager import BlobManager

    # Fetch APIs data
    df_apis = fetch_apis()
    logical_date = context["logical_date"]
    # Save to Azure Blob Storage
    blob_manager = BlobManager.from_connection_id("azure_blob_storage", container_name="apim")
    blob_manager.upload_blob(
        blob_name=f"apim/{logical_date.format("%Y%m%d")}/apis.parquet",
        data=df_apis.to_parquet(index=False),
        metadata={"producer": "airflow", "asset": "apim_apis", "data_group": "apim", "reference_date": logical_date.format("%Y-%m-%d")}
        )

    yield Metadata(extra={"reference_date": logical_date.isoformat()})
