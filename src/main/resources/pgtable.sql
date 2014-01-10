CREATE TABLE %tableName%
(
  id uuid NOT NULL,
  content bytea NOT NULL,
  state integer,
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


CREATE TABLE %tableName%_tags
(
  id uuid NOT NULL,
  tag character varying(65),
  CONSTRAINT %tableName%_tags_pkey PRIMARY KEY (id, tag)
)
WITH (
    OIDS=FALSE
);

ALTER TABLE %tableName%_tags
  ADD FOREIGN KEY (id) REFERENCES %tableName% (id) ON UPDATE CASCADE ON DELETE CASCADE;

