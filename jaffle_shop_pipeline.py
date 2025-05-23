import dlt
import requests

# CORRECCIÓN DEFINITIVA:
# 1. La rama es 'main'.
# 2. Los archivos ya no tienen el prefijo 'raw_'.
# Update the BASE_URL to the correct path within the repository
BASE_URL = "https://github.com/dlt-hub/fast-api-jaffle-shop/tree/main/seed/"
CUSTOMERS_URL = f"{BASE_URL}raw_customers.csv"
ORDERS_URL = f"{BASE_URL}raw_orders.csv"

# --- Test URLs before running pipeline ---
# REMOVE THE TRIPLE QUOTES TO UNCOMMENT THIS BLOCK
try:
    print(f"Testing URL: {CUSTOMERS_URL}")
    response_customers = requests.get(CUSTOMERS_URL)
    # This line will raise an HTTPError for bad responses (like 404 Not Found)
    response_customers.raise_for_status()
    print(f"Successfully accessed {CUSTOMERS_URL}")

    print(f"Testing URL: {ORDERS_URL}")
    response_orders = requests.get(ORDERS_URL)
    # This line will raise an HTTPError for bad responses (like 404 Not Found)
    response_orders.raise_for_status()
    print(f"Successfully accessed {ORDERS_URL}")

except requests.exceptions.HTTPError as e:
    print(f"Error accessing URL: {e}")
    # Exit the script if URLs are not accessible. This prevents the pipeline from running
    # with invalid URLs.
    # Consider using sys.exit() or raising a SystemExit exception instead of exit()
    # for cleaner exit in a script, although exit() works in IPython.
    exit()
# --- End of Test ---


def get_data_from_url(url):
    """Obtiene los datos CSV desde una URL y los procesa."""
    response = requests.get(url)
    # Esta línea se asegura de que la petición fue exitosa (código 200)
    response.raise_for_status()
    return response.text

@dlt.source
def jaffle_shop_source():
    """Define la fuente de datos para los clientes de Jaffle Shop."""
    @dlt.resource(name="customers", write_disposition="replace")
    def customers_resource():
        yield get_data_from_url(CUSTOMERS_URL)

    @dlt.resource(name="orders", write_disposition="replace")
    def orders_resource():
        yield get_data_from_url(ORDERS_URL)

    return (customers_resource, orders_resource)


# We are running the pipeline only if the script is executed directly.
if __name__ == "__main__":
    # Configura el pipeline
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop",
        destination="duckdb",
        dataset_name="jaffle_data"
    )

    # Ejecuta el pipeline
    print("Cargando datos de Jaffle Shop...")
    load_info = pipeline.run(jaffle_shop_source())

    # Imprime información sobre la carga
    print(load_info)
    print("¡Datos cargados exitosamente!")