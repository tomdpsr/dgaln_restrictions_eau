DROP TABLE IF EXISTS analytics_arrete;

CREATE TABLE analytics_arrete
(
    numero_arrete                 text,
    date_signature                date,
    debut_validite_arrete         date,
    fin_validite_arrete           date,
    duree_jours                   float
);


DROP TABLE IF EXISTS analytics_niveau;

CREATE TABLE analytics_niveau
(
    code_iso_departement varchar(10),
    date_jour            date,
    numero_niveau        smallint,
    surface_zone         float,
    type_zone            char(3)
);

