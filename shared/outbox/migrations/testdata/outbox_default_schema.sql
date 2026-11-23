-- Snapshot canônico do schema resultante de sharedOutbox.Apply(dsn) SEM opts.
-- Serve de baseline para TestApplyDefaultBackwardCompatible: garante que o
-- DDL do caminho default não mudou depois que a migration virou template.
--
-- Gerado via: docker exec gateway-db pg_dump -U gateway -d gateway_db \
--             --schema-only --table=outbox --no-owner --no-privileges
-- Filtragem: removidas linhas \restrict/\unrestrict (nonce por sessão do pg_dump 16.14),
-- linhas SET (config-específicas), comentários de versão, e header/footer de dump.
-- Ordem preservada; DDL efetivo intacto.

CREATE TABLE public.outbox (
    event_id uuid NOT NULL,
    event_type character varying(255) NOT NULL,
    payload jsonb NOT NULL,
    correlation_id character varying(255),
    status character varying(20) DEFAULT 'PENDING'::character varying NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    sent_at timestamp with time zone,
    CONSTRAINT outbox_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'SENT'::character varying, 'DEAD'::character varying])::text[])))
);

ALTER TABLE ONLY public.outbox
    ADD CONSTRAINT outbox_pkey PRIMARY KEY (event_id);

CREATE INDEX idx_outbox_pending ON public.outbox USING btree (created_at) WHERE ((status)::text = 'PENDING'::text);
