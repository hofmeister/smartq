CREATE TABLE %tableName%
(
  id uuid NOT NULL,
  content bytea NOT NULL,
  state integer,
  type character varying(65),
  priority integer,
  estimate bigint,
  CONSTRAINT %tableName%_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX %tableName%_state_priority_idx
  ON %tableName%
  USING btree
  (state, priority DESC);

CREATE INDEX %tableName%_state_type_priority_idx
  ON %tableName%
  USING btree
  (state, type COLLATE pg_catalog."default", priority DESC);

