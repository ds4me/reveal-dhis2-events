create schema if not exists dhis;

DROP TABLE IF EXISTS dhis.datavalues;
CREATE TABLE dhis.datavalues
(
    dhis_event_uid character varying(11) ,
    dhis_dataelement_uid character varying(11),
    dhis_datavalue character varying(100) 
);

ALTER TABLE dhis.datavalues   OWNER to postgres;
GRANT ALL ON TABLE dhis.datavalues TO postgres;
GRANT INSERT, SELECT, TRUNCATE ON TABLE dhis.datavalues TO dhis2;

DROP TABLE IF EXISTS dhis.metadata;
CREATE TABLE dhis.metadata
(
    id serial primary key,
    reveal_plan_uuid character varying(40), 
    reveal_view_to_use character varying(255), 
    dhis2_event_program_uid character varying(11), 
    dhis2_event_program_stage_uid character varying(11),
    orgunit_levels_above_oa integer
)

ALTER TABLE dhis.metadata     OWNER to postgres;
GRANT ALL ON TABLE dhis.metadata TO postgres;
GRANT INSERT, SELECT, TRUNCATE ON TABLE dhis.metadata TO dhis2;

drop table if exists dhis.events;
CREATE TABLE dhis.events
(
    dhis_uid character varying(11),
    reveal_uuid character varying(36),
    reveal_date_created date,
    reveal_jurisdiction_id character varying(36),
    reveal_jurisdiction_parent_id character varying(36)
);

ALTER TABLE dhis.events    OWNER to postgres;
GRANT ALL ON TABLE dhis.events TO postgres;
GRANT INSERT, SELECT, TRUNCATE ON TABLE dhis.events TO dhis2;
insert into dhis.events select * from namibia_prod.dhis2_events ;

DROP TABLE IF EXISTS dhis.orgunits;
CREATE TABLE dhis.orgunits
(
    id  serial primary key,
    dhis_orgunit_uid character varying(50),
    dhis_orgunit_name character varying(50) ,
    reveal_uid character varying(50)
);
ALTER TABLE dhis.orgunits    OWNER to postgres;
GRANT ALL ON TABLE dhis.orgunits TO postgres;
GRANT INSERT, SELECT, TRUNCATE ON TABLE dhis.orgunits TO dhis2;
insert into dhis.orgunits select * from namibia_prod.dhis2_orgunits;

DROP TABLE IF EXISTS dhis.json_export;
CREATE TABLE dhis.json_export
(
    id  serial primary key,
    jsontext character varying
);
ALTER TABLE dhis.json_export    OWNER to postgres;
GRANT ALL ON TABLE dhis.json_export TO postgres;
GRANT INSERT, SELECT, TRUNCATE ON TABLE dhis.json_export TO dhis2;

DROP TABLE IF EXISTS dhis.event_data_values_pivot;
CREATE TABLE dhis.event_data_values_pivot
(
    dhis_event_uid character varying(11),
    datavalues text 
);
ALTER TABLE dhis.event_data_values_pivot    OWNER to postgres;
GRANT ALL ON TABLE dhis.event_data_values_pivot TO postgres;
GRANT INSERT, SELECT, TRUNCATE ON TABLE dhis.event_data_values_pivot TO dhis2;
insert into dhis.event_data_values_pivot select * from namibia_prod.dhis2_event_data_values_pivot;

DROP TABLE IF EXISTS dhis.disaggregations;
CREATE TABLE dhis.disaggregations
(
    id serial primary key,
    uid character varying(15),
    name character varying(50),
    reveal_uuid character varying(40),
    active boolean NOT NULL DEFAULT true
);
ALTER TABLE dhis.disaggregations    OWNER to postgres;
GRANT ALL ON TABLE dhis.disaggregations TO postgres;
GRANT INSERT, SELECT, TRUNCATE ON TABLE dhis.disaggregations TO dhis2;
insert into dhis.disaggregations select * from namibia_prod.dhis2_disaggregations ;

CREATE TABLE dhis.dataelements
(
    id serial primary key,
    uid character varying(15),
    name character varying(80),
    reveal_field character varying(50),
    reveal_calc character varying(255),
    reveal_datatype character varying(10)
);
ALTER TABLE dhis.dataelements    OWNER to postgres;
GRANT ALL ON TABLE dhis.dataelements TO postgres;
GRANT INSERT, SELECT, TRUNCATE ON TABLE dhis.disaggregations TO dhis2;
insert into dhis.dataelements (uid,name,reveal_field,reveal_calc,reveal_datatype) select uid,name,reveal_field,reveal_calc,reveal_datatype
	from namibia_prod.dhis2_dataelements ;
	

CREATE OR REPLACE FUNCTION dhis.populate_event_export(
	)
    RETURNS SETOF character varying 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$

declare sql_query 			varchar(8000);
declare from_clause 		varchar(8000);
declare counter 			int;
declare max_cols 			int;
declare col_name 			varchar(100);
declare uid 				varchar(14);
declare reveal_field 		varchar(100);
declare reveal_calc  		varchar(255);
declare reveal_datatype 	varchar(10);
declare datavalue_count 	int;
declare crosstab_result 	varchar(800);
declare crosstab_select 	varchar(800);
declare crosstab_full 		varchar(8000);
declare datavalue_concat 	varchar(8000);
declare max_id				int;

declare reveal_plan_uuid varchar(36);
declare reveal_view_to_use varchar(255);
declare dhis2_event_program_uid varchar(11);
declare dhis2_event_program_stage_uid varchar(11);
declare orgunit_levels_above_oa int;

BEGIN
	-- get metadata
	select into 	reveal_plan_uuid,reveal_view_to_use,dhis2_event_program_uid,dhis2_event_program_stage_uid,orgunit_levels_above_oa
					v.reveal_plan_uuid,v.reveal_view_to_use,v.dhis2_event_program_uid,v.dhis2_event_program_stage_uid,v.orgunit_levels_above_oa
	from 			dhis.metadata v;
	
	-- temporary table to hold narrow transformed values
	crosstab_select = 'select dhis_event_uid, dhis_dataelement_uid, cast(''''{"dataElement": "''''||dhis_dataelement_uid || ''''", "value": "'''' || dhis_datavalue::varchar(100)||''''"}'''' as varchar(100)) from dhis.datavalues order by 1,2';
	crosstab_result = 'dhis_event_uid varchar(11)';
	datavalue_concat = '';
	
	truncate table dhis.datavalues;

	--ensure every reveal event has a dhis2 event id
	sql_query = 'insert into dhis.events (dhis_uid, reveal_uuid, reveal_date_created, reveal_jurisdiction_id, reveal_jurisdiction_parent_id)' ;
	sql_query = sql_query || ' select 		dhis.uid(), event_id,created_at, jurisdiction_id, jurisdiction_parent_id';
	sql_query = sql_query || ' from 		' || reveal_view_to_use || ' ex';
	sql_query = sql_query || ' left join 	dhis.events e on e.reveal_uuid = ex.event_id';
	sql_query = sql_query || ' where 		e.reveal_uuid is null;';
	raise notice 'sql: % ', sql_query;
	execute sql_query;
	
	--itterate through dhis2 data elements, transform values and generate dynamic sql 
	counter = 1;
	select into max_cols count(*) from dhis.dataelements;
	raise notice 'col count: % ', cast(max_cols as varchar(40)) ;
	while counter <= max_cols
	loop
		-- set variables
		select into uid, reveal_field, reveal_calc, reveal_datatype
					de.uid, de.reveal_field, de.reveal_calc, de.reveal_datatype 
		from 		dhis.dataelements de
		where 		id = counter;

		--insert into entity-attribute-value narrow table
		if coalesce(reveal_field,reveal_calc) is not null then
			raise notice ' uid %', uid;
			sql_query = 'insert into dhis.datavalues (dhis_event_uid,dhis_dataelement_uid,dhis_datavalue) select ';
			sql_query = sql_query || ' e.dhis_uid   ,' || '''' || uid || '''  ,' || coalesce(reveal_field,reveal_calc);
			--sql_query = sql_query || ' from namibia_prod.vw_namibia_irs_for_export ex';
			sql_query = sql_query || ' from ' || reveal_view_to_use ||' ex';
			sql_query = sql_query || ' inner join dhis.events e on e.reveal_uuid = ex.event_id';

			raise notice 'sql: % ', sql_query;
			execute sql_query;
			select into datavalue_count count(*) from dhis.datavalues;
			raise notice ' datavalue_count %', datavalue_count;

			crosstab_result = crosstab_result ||  ','|| uid ||  ' varchar(100) ';
			datavalue_concat = uid || ' || ' || ''',''' || ' || ' ||  datavalue_concat;
		end if;
		counter = counter +1;

	end loop;
	
	raise notice 'col crosstab_select: % ', crosstab_select;
	raise notice 'col datavalue_concat: % ', datavalue_concat;
	
	--create the dynamic table for the pivoted data (each column with the json for a dataelement-value tuple)
	drop table if exists dhis.event_crosstab;
	sql_query = 'create table dhis.event_crosstab as select * from crosstab( '''||crosstab_select||''') as final_result('||crosstab_result||');';
	raise notice 'sql: % ', sql_query;
	execute sql_query;
	
	--concatenate the datavalue columns into a json string per event 
	drop table if exists dhis.event_data_values_pivot;
	sql_query = 'create  table dhis.event_data_values_pivot as select dhis_event_uid, ' || datavalue_concat || ''''' datavalues from  dhis.event_crosstab' ;
	raise notice 'sql: % ', sql_query;
	execute sql_query  ;

	--select * from dhis.dhis2_event_data_values_pivot limit 100;;
	
	--concatenate all other json elements into full json for each event
	truncate table dhis.json_export;
	insert into dhis.json_export (jsontext)  values ( '"events" : [');
	
	insert into dhis.json_export (jsontext) 
	select 	'{' ||
			'"orgUnit": "' || 	o.dhis_orgunit_uid || '"' || ',' ||
			'"status": "ACTIVE"'|| ',' ||
			'"program": "'||dhis2_event_program_uid||'"'|| ',' ||
			'"programStage": "'||dhis2_event_program_stage_uid||'"'|| ',' ||
			'"eventDate": "'||	de.reveal_date_created::date::text||'"'|| ',' ||
			'"event": "' ||		c.dhis_event_uid || '"'|| ',' ||
			'"dataValues": [' ||regexp_replace(c.datavalues, ',$','') || ']'|| ',' ||
			'"attributeCategoryOptions": "'||d.uid||'"' ||
			'},' 
	from 	 	dhis.event_data_values_pivot c
	inner join 	dhis.events de 			on de.dhis_uid = c.dhis_event_uid
	inner join 	dhis.disaggregations d 	on d.reveal_uuid = de.reveal_jurisdiction_id and d.active = true
	inner join 	dhis.orgunits o 		on o.reveal_uid = de.reveal_jurisdiction_parent_id;

	--strip training comma
	select into max_id max(id) from dhis.json_export;
	raise notice 'maxid : % ', max_id;
	update dhis.json_export set jsontext = substring(jsontext, 1, length(jsontext)-1) where id = max_id ;
	
	insert into dhis.json_export (jsontext) values (']}');
	sql_query = 'select cast(jsontext as varchar(4000)) as jsontext from dhis.json_export';
	raise notice 'sql: % ', sql_query;
	return query execute sql_query;

END;

$BODY$;

ALTER FUNCTION dhis.populate_event_export()
    OWNER TO postgres;
