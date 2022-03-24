import helper_functions

helper_functions.webflow_insert(
	data=helper_functions.posgres_raw(
		query_sql="""
			SELECT
				property_id as "property-id",
				company_id as "company-id",
				region_id as "region-id",
				portfolio_id as "portfolio-id",
				bun,
				division,
				region_name as "region-name",
				portfolio_name as "portfolio-name",
				name,
				phone as "contact-phone",
				email as "contact-email-3",
				address_street as "address-street",
				address_city as "address-city",
				address_state as "address-state",
				address_county as "address-county",
				CAST(address_zip as varchar(255)) as "address-zip"
			FROM
				property
			WHERE
				company_id = 165
			LIMIT
				10
		""",
	),
	collection_id="623107ba68bd7ba11ca033c7",
	input_type="csv",
	upsert_on="property-id",
)
