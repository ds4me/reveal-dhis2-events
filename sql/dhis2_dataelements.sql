CREATE TABLE namibia_prod.dhis2_dataelements
(
    id serial not null,
    uid character varying(15), 
    name character varying(80), 
    reveal_field character varying(50),
    reveal_calc character varying(255),
    reveal_datatype character varying(10) ,
    CONSTRAINT dhis2_dataelemets_pkey PRIMARY KEY (id)
);

ALTER TABLE namibia_prod.dhis2_dataelements
    OWNER to postgres;
