from typing import Dict
import pandas as pd
import pycountry


def get_country_map() -> Dict[str, str]:
    """
    Returns a dictionary mapping ISO 3166-1 numeric country codes (as strings)
    to their corresponding official country names.

    Returns:
        Dict[str, str]: A dictionary with numeric ISO country codes as string keys
                        and country names as values.
    """
    country_map = {}
    for country in pycountry.countries:
        if hasattr(country, 'numeric'):
            code = f"{int(country.numeric):03d}"  # Ensure 3-digit string
            country_map[code] = country.name
    return country_map


def get_country_map_df() -> pd.DataFrame:
    """
    Returns a pandas DataFrame with two columns: 'country_code' and 'country_name'.
    The 'country_code' column contains ISO 3166-1 numeric country codes as strings,
    and the 'country_name' column contains the corresponding official country names.

    Returns:
        pd.DataFrame: A DataFrame with two columns: 'country_code' and 'country_name'.
    """
    country_map = get_country_map()
    df = pd.DataFrame(list(country_map.items()), columns=['country_code', 'country_name'])
    return df


# Example usage
if __name__ == "__main__":
    country_map = get_country_map_df()
    print(country_map)
