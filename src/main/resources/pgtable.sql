CREATE TABLE %tableName%
(
  id uuid NOT NULL,
  content bytea NOT NULL,
  state integer,
  priority integer,
  created bigint,
  "order" SERIAL,
  referenceid character varying(45),
  "type" character varying(65),
  CONSTRAINT %tableName%_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);

CREATE INDEX %tableName%_referenceid_idx
  ON %tableName%
  USING btree (referenceid, priority DESC, created ASC, "order" ASC);

CREATE INDEX %tableName%_referenceid_reverse_idx
  ON %tableName%
  USING btree (referenceid, priority ASC, created DESC, "order" DESC);

CREATE INDEX %tableName%_state_priority_idx
  ON %tableName%
  USING btree
  (state DESC, priority DESC, created ASC, "order" ASC);


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


CREATE TABLE %tableName%_estimates
(

  "type" character varying(32) NOT NULL,
  duration bigint,
  "order" SERIAL,
  CONSTRAINT %tableName%_estimates_pkey PRIMARY KEY ("type", "order")
)
WITH (
    OIDS=FALSE
);


CREATE TABLE %tableName%_ratelimits
(

  "tag" character varying(32) NOT NULL,
  ratelimit bigint,
  CONSTRAINT %tableName%_ratelimits_pkey PRIMARY KEY (tag)
)
WITH (
    OIDS=FALSE
);

CREATE TABLE %tableName%_retrylimits
(

  "tag" character varying(32) NOT NULL,
  retrylimit bigint,
  CONSTRAINT %tableName%_retrylimits_pkey PRIMARY KEY (tag)
)
WITH (
    OIDS=FALSE
);

