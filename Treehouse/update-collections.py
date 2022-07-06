import sys
import database

configKwargs = {"filename": "./settings.ini"}

database.Webflow.insert(
	data=database.Postgres.raw(
		query_sql="""
			SELECT
				property.property_id as "property-id",
				property.company_id as "company-id",
				property.region_id as "region-id",
				property.portfolio_id as "portfolio-id",
				property.bun,
				division.name as division,
				region.name as "region-name",
				property.portfolio_name as "portfolio-name",
				property.name,
				property.phone as "contact-phone",
				property.email as "contact-email-3",
				property.address_street as "address-street",
				property.address_city as "address-city",
				property.address_state as "address-state",
				property.address_county as "address-county",
				CAST(property.address_zip as varchar(255)) as "address-zip"
			FROM
				property
				LEFT JOIN division ON (property.division_id = division.division_id)
				LEFT JOIN region ON (property.region_id = region.region_id)
			WHERE
				property.company_id = 165
			LIMIT
				10
		""", as_dict=True, configKwargs=configKwargs,
	),
	collection_id="623107ba68bd7ba11ca033c7",
	input_type="csv",
	upsert_on="property-id",
	configKwargs=configKwargs,
)
