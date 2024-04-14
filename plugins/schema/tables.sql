DROP TABLE IF EXISTS arrete;

CREATE TABLE arrete
(
    unique_key_arrete_zone_alerte text primary key,
    id_arrete                     int,
    id_zone                       int,
    numero_arrete                 text,
    numero_arrete_cadre           text,
    date_signature                date,
    debut_validite_arrete         date,
    fin_validite_arrete           date,
    numero_niveau                 smallint,
    nom_niveau                    text,
    statut_arrete                 text,
    chemin_fichier                text,
    chemin_fichier_arrete_cadre   text
);


DROP TABLE IF EXISTS public.zone_alerte;

CREATE TABLE public.zone_alerte
(
    id_zone              int primary key,
    code_zone            varchar(20),
    type_zone            char(3),
    nom_zone             text,
    surface_zone         float,
    numero_version       int,
    est_version_actuelle boolean,
    code_departement     varchar(5),
    code_iso_departement varchar(10),
    nom_departement      text,
    surface_departement  float,
    nom_bassin_versant   text
);


DROP TABLE IF EXISTS public.departement;

CREATE TABLE public.departement
(
    code_departement     char(3),
    nom_departement      varchar(50),
    est_valide           boolean
);
