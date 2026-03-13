"""
HubSpot API client for company search and property updates.

Required scopes on your HubSpot Private App:
  - crm.objects.companies.read
  - crm.objects.companies.write
  - crm.schemas.companies.read
"""

from typing import Optional, Set, List
import httpx
from domain_utils import normalize_domain

BASE_URL = "https://api.hubapi.com"


class HubSpotClient:
    def __init__(self, access_token: str):
        self.token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------ #
    #  Property introspection
    # ------------------------------------------------------------------ #

    async def get_property(self, property_name: str) -> dict:
        """
        Fetch a company property definition from HubSpot.
        Returns the property metadata including fieldType and options.
        """
        url = f"{BASE_URL}/crm/v3/properties/companies/{property_name}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=self.headers)
            resp.raise_for_status()
            return resp.json()

    async def get_tech_stack_property(self) -> dict:
        """Fetch the 'tech_stack' property definition from HubSpot."""
        return await self.get_property("tech_stack")

    async def get_valid_tech_stack_values(self) -> Set[str]:
        """Return the set of allowed dropdown/enum values."""
        prop = await self.get_tech_stack_property()
        options = prop.get("options", [])
        return {opt["value"] for opt in options}

    async def get_tech_stack_field_type(self) -> str:
        """Return the fieldType of the tech_stack property (e.g. 'select' or 'checkbox')."""
        prop = await self.get_tech_stack_property()
        return prop.get("fieldType", "select")

    async def get_valid_data_source_values(self) -> Set[str]:
        """Return the set of allowed values for data_source_tool property."""
        prop = await self.get_property("data_source_tool")
        options = prop.get("options", [])
        return {opt["value"] for opt in options}

    async def validate_properties(self) -> dict:
        """
        Validate that all required HubSpot properties exist and are accessible.
        Returns a dict with status for each property.
        """
        properties = {
            "tech_stack": {"label": "Tech Stack", "excel_column": "Technology"},
            "company_name___lead_gen": {"label": "Company Name - Lead Gen", "excel_column": "Company Name"},
            "data_source_tool": {"label": "Data Source Tool", "excel_column": "Source"},
        }
        results = {}
        async with httpx.AsyncClient() as client:
            for prop_name, info in properties.items():
                try:
                    url = f"{BASE_URL}/crm/v3/properties/companies/{prop_name}"
                    resp = await client.get(url, headers=self.headers)
                    if resp.status_code == 200:
                        data = resp.json()
                        results[prop_name] = {
                            "status": "connected",
                            "label": data.get("label", info["label"]),
                            "fieldType": data.get("fieldType", "unknown"),
                            "type": data.get("type", "unknown"),
                            "excel_column": info["excel_column"],
                        }
                    else:
                        results[prop_name] = {
                            "status": "error",
                            "label": info["label"],
                            "excel_column": info["excel_column"],
                            "error": f"HTTP {resp.status_code}: Property not found or not accessible",
                        }
                except Exception as e:
                    results[prop_name] = {
                        "status": "error",
                        "label": info["label"],
                        "excel_column": info["excel_column"],
                        "error": str(e),
                    }
        return results

    # ------------------------------------------------------------------ #
    #  Company search by domain
    # ------------------------------------------------------------------ #

    async def search_company_by_domain(self, domain: str) -> List[dict]:
        """
        Search HubSpot companies using the Website URL property as the
        primary identifier. Also checks the domain property as fallback.

        Handles URL variations: www.example.com, example.com, https://example.com
        """
        normalized = normalize_domain(domain)
        if not normalized:
            return []

        url = f"{BASE_URL}/crm/v3/objects/companies/search"

        # Website URL is the primary match field.
        # Search multiple variations to handle different URL formats stored in HubSpot.
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "website",
                            "operator": "CONTAINS_TOKEN",
                            "value": normalized,
                        }
                    ]
                },
                {
                    "filters": [
                        {
                            "propertyName": "website",
                            "operator": "EQ",
                            "value": normalized,
                        }
                    ]
                },
                {
                    "filters": [
                        {
                            "propertyName": "website",
                            "operator": "EQ",
                            "value": f"www.{normalized}",
                        }
                    ]
                },
                {
                    "filters": [
                        {
                            "propertyName": "website",
                            "operator": "EQ",
                            "value": f"https://{normalized}",
                        }
                    ]
                },
                {
                    "filters": [
                        {
                            "propertyName": "website",
                            "operator": "EQ",
                            "value": f"https://www.{normalized}",
                        }
                    ]
                },
            ],
            "properties": ["name", "domain", "website", "tech_stack", "company_name___lead_gen", "data_source_tool"],
            "limit": 10,
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(url, headers=self.headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])

            # If no match on website, fallback to domain property
            if not results:
                fallback_payload = {
                    "filterGroups": [
                        {
                            "filters": [
                                {
                                    "propertyName": "domain",
                                    "operator": "EQ",
                                    "value": normalized,
                                }
                            ]
                        },
                    ],
                    "properties": ["name", "domain", "website", "tech_stack", "company_name___lead_gen", "data_source_tool"],
                    "limit": 10,
                }
                resp2 = await client.post(url, headers=self.headers, json=fallback_payload)
                resp2.raise_for_status()
                results = resp2.json().get("results", [])

            return results

    # ------------------------------------------------------------------ #
    #  Update tech stack
    # ------------------------------------------------------------------ #

    async def update_company_properties(
        self,
        company_id: str,
        tech_value: str,
        tech_field_type: str,
        current_tech: Optional[str],
        company_name: str,
        source: str,
        current_data_source: Optional[str] = None,
    ) -> dict:
        """
        Update multiple properties on a HubSpot company:
          - tech_stack (select or checkbox)
          - company_name___lead_gen (text)
          - data_source_tool (checkbox / multi-select)
        """
        url = f"{BASE_URL}/crm/v3/objects/companies/{company_id}"

        # Tech stack value
        if tech_field_type == "checkbox" and current_tech:
            existing = set(v.strip() for v in current_tech.split(";"))
            existing.add(tech_value)
            tech_final = ";".join(sorted(existing))
        else:
            tech_final = tech_value

        # Data source tool (checkbox / multi-select) — append if not present
        if current_data_source:
            existing_sources = set(v.strip() for v in current_data_source.split(";"))
            existing_sources.add(source)
            source_final = ";".join(sorted(existing_sources))
        else:
            source_final = source

        payload = {
            "properties": {
                "tech_stack": tech_final,
                "company_name___lead_gen": company_name,
                "data_source_tool": source_final,
            }
        }

        async with httpx.AsyncClient() as client:
            resp = await client.patch(url, headers=self.headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    async def update_tech_stack(
        self, company_id: str, tech_value: str, field_type: str, current_value: Optional[str] = None
    ) -> dict:
        """Legacy method — use update_company_properties instead."""
        url = f"{BASE_URL}/crm/v3/objects/companies/{company_id}"

        if field_type == "checkbox" and current_value:
            existing = set(v.strip() for v in current_value.split(";"))
            existing.add(tech_value)
            final_value = ";".join(sorted(existing))
        else:
            final_value = tech_value

        payload = {
            "properties": {
                "tech_stack": final_value,
            }
        }

        async with httpx.AsyncClient() as client:
            resp = await client.patch(url, headers=self.headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    # ------------------------------------------------------------------ #
    #  Connection test
    # ------------------------------------------------------------------ #

    async def test_connection(self) -> bool:
        """Verify the token works by fetching account info."""
        url = f"{BASE_URL}/crm/v3/properties/companies/domain"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=self.headers)
            return resp.status_code == 200
