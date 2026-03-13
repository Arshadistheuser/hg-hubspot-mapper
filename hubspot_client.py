"""
HubSpot API client for company search and property updates.

Required scopes on your HubSpot Private App:
  - crm.objects.companies.read
  - crm.objects.companies.write
  - crm.schemas.companies.read
"""

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

    async def get_tech_stack_property(self) -> dict:
        """
        Fetch the 'tech_stack' property definition from HubSpot.
        Returns the property metadata including fieldType and options.
        """
        url = f"{BASE_URL}/crm/v3/properties/companies/tech_stack"
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=self.headers)
            resp.raise_for_status()
            return resp.json()

    async def get_valid_tech_stack_values(self) -> set[str]:
        """Return the set of allowed dropdown/enum values (lowercased for comparison)."""
        prop = await self.get_tech_stack_property()
        options = prop.get("options", [])
        return {opt["value"] for opt in options}

    async def get_tech_stack_field_type(self) -> str:
        """Return the fieldType of the tech_stack property (e.g. 'select' or 'checkbox')."""
        prop = await self.get_tech_stack_property()
        return prop.get("fieldType", "select")

    # ------------------------------------------------------------------ #
    #  Company search by domain
    # ------------------------------------------------------------------ #

    async def search_company_by_domain(self, domain: str) -> list[dict]:
        """
        Search HubSpot companies whose 'domain' or 'website' contains
        the normalized domain. Returns a list of matching company records.

        HubSpot stores domains in the 'domain' property (primary) and
        sometimes in 'website'. We search both using filter groups (OR).
        """
        normalized = normalize_domain(domain)
        if not normalized:
            return []

        url = f"{BASE_URL}/crm/v3/objects/companies/search"

        # Search with CONTAINS_TOKEN on domain property and also
        # use EQ for exact match — HubSpot domain field stores bare domains
        payload = {
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
                {
                    "filters": [
                        {
                            "propertyName": "domain",
                            "operator": "EQ",
                            "value": f"www.{normalized}",
                        }
                    ]
                },
                {
                    "filters": [
                        {
                            "propertyName": "website",
                            "operator": "CONTAINS_TOKEN",
                            "value": normalized,
                        }
                    ]
                },
            ],
            "properties": ["name", "domain", "website", "tech_stack"],
            "limit": 10,
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(url, headers=self.headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])

    # ------------------------------------------------------------------ #
    #  Update tech stack
    # ------------------------------------------------------------------ #

    async def update_tech_stack(
        self, company_id: str, tech_value: str, field_type: str, current_value: str | None = None
    ) -> dict:
        """
        Update the tech_stack property on a company.

        If field_type is 'checkbox' (multi-select), appends the new value
        using semicolon-separated format. Otherwise overwrites.
        """
        url = f"{BASE_URL}/crm/v3/objects/companies/{company_id}"

        if field_type == "checkbox" and current_value:
            # Multi-select: append if not already present
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
