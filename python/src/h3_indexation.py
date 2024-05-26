
import os
import time
from datetime import datetime
import numpy as np
from scipy.sparse import coo_matrix

# Intake
from dotenv import load_dotenv
from intake import open_catalog

# Pandas
import pandas as pd
import geopandas as gpd

# H3
import h3
from h3pandas.util import shapely
from tobler.util import h3fy

# SQL
from sqlalchemy import *
from geoalchemy2 import Geometry, WKTElement

# Dask
import dask_geopandas as ddg
from dask.distributed import Client

# %% [markdown]
# # Constantes

# %%
# Aire moyenne de chaque hexagone (de la résolution 0 à 15) en Km2
hex_area=[4250546.8477000, 607220.9782429, 86745.8540347, 12392.2648621, 1770.3235517, 252.9033645, 36.1290521,
              5.1612932, 0.7373276, 0.1053325, 0.0150475, 0.0021496, 0.0003071, 0.0000439, 0.0000063, 0.0000009]

# %%
# Chargement des constantes d'environnement
load_dotenv()

usr=os.getenv("DB_USER")
pswd=os.getenv("DB_PWD")
host=os.getenv("DB_HOST")
port=os.getenv("DB_PORT")
home=os.getenv("HOME_PATH")
db_traitement=os.getenv("DB_WORKSPACE")
db_ref=os.getenv("DB_REF")
db_externe=os.getenv("DB_EXT")
dwh_fact_strategy=os.getenv("DWH_FACT_STRATEGY")
dwh_dim_strategy=os.getenv("DWH_DIM_STRATEGY")


commun_path=os.getenv("COMMUN_PATH")
project_dir=os.getenv("PROJECT_PATH")
data_catalog_dir=os.getenv("DATA_CATALOG_DIR")
data_output_dir=os.getenv("DATA_OUTPUT_DIR")
sig_data_path=os.getenv("SIG_DATA_PATH")
db_workspace=os.getenv("DB_WORKSPACE")
db_workspace=os.getenv("DB_REF")

# %% [markdown]
# # Fonctions

# %% [markdown]
# ## Fonctions Dask

# %% [markdown]
# ### Récupération du client

# %%
# Récupération du scheduler orchestrant les workers
client = Client() # Scheduler / Workers locaux  "192.168.1.24:8786"

# %%
client # Infos sur le client

# %%
# Fermeture du client
client.close()

# %% [markdown]
# ### Fonctions

# %%
def indexation_dask(gdf, npartitions, resolution):
    """
    Fonction retournant un DataFrame après ajout d'une colonne hex_id en index.
    En d'autres termes, cette fonction indexe un GeoDataFrame sur une grille uniforme.
    Elle utilise les procédés de parallélisation et de clustering de la librairie Dask 
    afin d'accélérer les temps de calculs.

    param gdf: GeoDataFrame en entrée
    param npartitions: nombre de tâches à effectuer en parallèle
    param resolution: résolution des hexagones

    return: DataFrame 

    """
    # Structure du DataFrame renvoyé en sortie
    # On ne conserve pas la colonne géométrie
    df_meta = pd.DataFrame(columns=list(gdf.columns))
    df_meta.drop(columns=[gdf.geometry.name], inplace=True)
    df_meta.index.names = ['hex_id']
    
    data = ddg.from_geopandas(gdf,npartitions)
    gdf_map = data.map_partitions(func=indexation, resolution=resolution, meta=df_meta)
    client.persist(gdf_map)
    return gdf_map.compute()

# %%
def compact_dask(tab_name, schema, gdf, tx_spatial, res_min, res_max, nb_cluster=1, i_start=1, fast=False):
    """
    Fonction indexant un GeoDataFrame sur une grille adaptative et persistant le résultat dans une base de données PostGIS.
    Attention, le géodataframe en entrée ne doit pas correspondre à une partition de l'espace.
    Pour cela, utiliser compact_dask_partition.
    Elle utilise les procédés de parallélisation et de clustering de la librairie Dask 
    afin d'accélérer les temps de calculs.

    param tab_name: nom de la table de sortie au sein de la base de donnée PostGIS
    param schema: schéma contenant la table de sortie
    param gdf: GeoDataFrame en entrée
    param tx_spatial: taux minimal de remplissage d'une zone de données pour chaque échelle de résolution
        (sous forme de dictionnaire)
    param res_min: résolution minimale des hexagones
    param res_max: résolution maximale des hexagones
    param nb_cluster: nombre de bloc de données traités successivement
    param i_start: indice (commençant à 1) du bloc / cluster de départ
        Ce paramètre est utile lorsqu'il s'agit de reprende un traitement.
    param fast: Si fast=False, l'algorithme est plus lent mais l'indexation se fait au plus juste.
        Si fast=True, l'algorithme est rapide mais certaines périphéries d'objets risquent de ne pas être indexées.

    """
    if nb_cluster > len(gdf):
        nb_cluster = len(gdf)

    with open('suivi.txt', 'a') as f:
            f.write(f"{datetime.now()} (compact_dask)\nnom: {schema}.{tab_name}\n")
            f.write(f"nb_cluster: {nb_cluster} | res: {res_min}-{res_max}\n")

    list_of_sep = np.linspace(0, len(gdf), num=nb_cluster+1, endpoint=True, dtype=int)
    for i in range (i_start-1, len(list_of_sep)-1):
        start_time = time.time()
        print('Cluster ' +  str((i+1)))

        gdf_cluster = gdf.iloc[list_of_sep[i]:list_of_sep[i+1]] # Sous-ensemble du gdf en entrée
        cluster_output = compact_dask_fct(gdf_cluster, len(gdf_cluster)-1, tx_spatial, res_min, res_max, fast)

        # Connexion à la base de données "oeil_traitement"
        connection = getEngine()    

        # Intégration de la table dans le DWH
        updateTable(cluster_output, tab_name, connection, schema, methode="append", geom=False)    
        connection.dispose()

        with open('suivi.txt', 'a') as f:
            f.write('Cluster ' +  str((i+1)) + '\n')
            f.write("   --- %s objets ---\n" % (len(cluster_output)))
            f.write("   --- %s secs ---\n" % (round((time.time() - start_time), 2)))

        print("   --- %s objets ---" % (len(cluster_output)))
        print("   --- %s secs ---" % (round((time.time() - start_time), 2))) 

    with open('suivi.txt', 'a') as f:
        f.write("\n") 

# %%
def compact_dask_partition(tab_name, schema, gdf, decoupage, nb_cluster, npartitions, colonne, tx, res_min, res_max, i_start=1, tx_spatial=0.5, debug=False):   
    """
    ##todo: rendre la couche découpage dynamique 

    Fonction indexant un GeoDataFrame sur une grille adaptative et persistant le résultat dans une base de données PostGIS.
    Attention, le géodataframe en entrée doit correspondre à une partition de l'espace.
    Elle utilise les procédés de parallélisation et de clustering de la librairie Dask 
    afin d'accélérer les temps de calculs.

    param tab_name: nom de la table de sortie au sein de la base de donnée PostGIS
    param schema: schéma contenant la table de sortie
    param gdf: GeoDataFrame en entrée
    param decoupage: GeoDataFrame composé des identifiants et de la géométrie des hexagones de plus faible résolution
        (= maillage hexagonal grossier de la donnée en entrée). Ce GeoDataFrame peut être obtenu via la fonction "segmentation".
    param nb_cluster: nombre de bloc de données traités successivement
    param npartitions: nombre de partitions utilisé par Dask pour la parralélisation
    param colonne: nom du champ à conserver à la fin de l'indexation
    param tx: taux minimal de remplissage d'une zone de données pour chaque échelle de résolution
        (sous forme de dictionnaire)
    param res_min: résolution minimale des hexagones
    param res_max: résolution maximale des hexagones
    param i_start: indice (commençant à 1) du bloc / cluster de départ
        Ce paramètre est utile lorsqu'il s'agit de reprende un traitement.
    param tx_spatial: taux de remplissage minimal de conservation d'un hexagone 
        (uniquement lors de la dernière itération de l'algorithme i.e. res=res_max)
    param debug: si vrai alors la sortie n'est pas persistée en base de donnée et le fichier de suivi n'est pas créé

    """
    gdf.index.names = ['index']
    if nb_cluster > len(decoupage):
        nb_cluster = len(decoupage)

    len_buffer = np.sqrt(hex_area[res_min]*1000000)*0.6 # 60% de la largeur d'un hexagone de granularité res_min

    if not debug:
        # Création du fichier de suivi
        with open('suivi.txt', 'a') as f:
                f.write(f"{datetime.now()} (compact_dask_partition)\nnom: {schema}.{tab_name}\n")
                f.write(f"nb_cluster: {nb_cluster} | npartitions: {npartitions} | colonne: {colonne} | res: {res_min}-{res_max}\n")

    list_of_sep = np.linspace(0, len(decoupage), num=nb_cluster+1, endpoint=True, dtype=int)
    for i in range (i_start-1, len(list_of_sep)-1):
        start_time = time.time()
        print('Cluster ' +  str((i+1)))
        decoupage_cluster = decoupage.iloc[list_of_sep[i]:list_of_sep[i+1]]
        gdf_cluster = gpd.clip(gdf, decoupage_cluster["geometry"].buffer(len_buffer)) # Sous-ensemble du gdf en entrée
        cluster_output = compact_dask_partition_fct(gdf_cluster, decoupage_cluster, npartitions, colonne, tx, res_min, res_max, tx_spatial)

        if not debug:
            # Connexion à la base de données "oeil_traitement"
            connection = getEngine()    
            # Intégration de la table dans le DWH
            updateTable(cluster_output, tab_name, connection, schema, methode="append", geom=False)    
            connection.dispose()

            with open('suivi.txt', 'a') as f:
                f.write('Cluster ' +  str((i+1)) + '\n')
                f.write("   --- %s objets ---\n" % (len(cluster_output)))
                f.write("   --- %s secs ---\n" % (round((time.time() - start_time), 2)))

        print("   --- %s objets ---" % (len(cluster_output)))
        print("   --- %s secs ---" % (round((time.time() - start_time), 2)))
        
    if not debug:
        with open('suivi.txt', 'a') as f:
                f.write("\n")

# %% [markdown]
# ## Fonctions de traitements

# %%
def indexation(gdf, resolution):
    """
    Fonction retournant un DataFrame après ajout d'une colonne hex_id en index.
    En d'autres termes, cette fonction indexe un GeoDataFrame sur une grille uniforme.

    param gdf: GeoDataFrame en entrée
    param resolution: résolution des hexagones
    return: DataFrame

    """
    # Mise en conformité de la colonne "geometry"
    if(gdf.geometry.name != 'geometry'):
        gdf.rename_geometry('geometry',inplace=True)

    gdf = h3_polyfill(gdf.to_crs(epsg=4326), resolution) # Indexation du DataFrame
    df = pd.DataFrame(gdf.drop(columns={gdf.geometry.name})) # Suppression de la colonne "geometry"
    df = df.explode('hex_id') # Explosion des liste d'identifiants hexagonaux
    df.set_index('hex_id', inplace=True) # Définition de l'index
    
    return df

# %%
def compact(gdf, tx_spatial, res_min, res_max, fast=False):
    """
    Fonction indexant un GeoDataFrame sur une grille dynamique avec superpositions possibles d'objets.

    param gdf: GeoDataFrame en entrée
    param tx_spatial: taux minimal de remplissage d'une zone de données pour chaque échelle de résolution
        (sous forme de dictionnaire)
    param res_min: résolution minimale des hexagones
    param res_max: résolution maximale des hexagones
    param fast: Si fast=False, l'algorithme est plus lent mais l'indexation se fait au plus juste.
        Si fast=True, l'algorithme est rapide mais certaines périphéries d'objets risquent de ne pas être indexées.
    
    return: DataFrame

    """
    tx_spatial[res_max] = 0.5
    
    # Création d'un GeoDataFrame vide
    output = pd.DataFrame(columns=gdf.drop(columns=[gdf.geometry.name]).columns.tolist())
    output.index.names = ['hex_id']

    len_buffer = np.sqrt(hex_area[res_min]*1000000)*1 # largeur d'un hexagone de granularité res_min
    gdf_init = gpd.GeoSeries(gdf.unary_union.buffer(len_buffer), crs="EPSG:3163")
    hex = h3fy(gdf_init,res_min) # Segmentation en hexagones de la géométrie du gdf en entrée augmentée d'un buffer

    # Pour chaque objet
    for index_obj, row_obj in gdf.iterrows():
        res = res_min
        gdf_invalid = hex
        geom_obj = row_obj[gdf.geometry.name]

        while(res <= res_max):
            valid_cells = [] # Cellules n'ayant pas besoin d'être divisées
            valid_geom = []
            invalid_cells = [] # Cellules ayant besoin d'être divisées
            invalid_geom = []

            clip = to_children(gdf_invalid, res) # Récupération des enfants des cellules invalides
            for index_clip, row_clip in clip.iterrows():
                hex_geom = row_clip.geometry
                area = geom_obj.intersection(hex_geom).area
                if fast:
                    intersection = area
                else:
                    # On considère une zone 10% plus importante autour des hexagones
                    intersection = geom_obj.intersection(hex_geom.buffer(np.sqrt(hex_area[res]*1000000)*0.1)).area 
                if  intersection == 0:
                    continue
                elif area/hex_geom.area >= tx_spatial[res]:
                    valid_cells.append(index_clip)
                    valid_geom.append(hex_geom)
                elif res < res_max:
                    invalid_cells.append(index_clip)
                    invalid_geom.append(hex_geom)

            data = {}
            # Ajout des attributs de l'objet dans le DataFrame de sortie
            for key, value in row_obj.iteritems():
                if key != gdf.geometry.name:
                    data[key] = value
            valid_gdf = pd.DataFrame(data, index=valid_cells)
            valid_gdf.index.name = "hex_id"

            if res < res_max:
                gdf_invalid = clip.loc[invalid_cells]

            output = pd.concat([output, valid_gdf], ignore_index=False) 
            res+=1
    return output

# %% [markdown]
# ## Fonctions BDD

# %%
def getEngine(user=usr, pswd=pswd, host=host, dbase=db_traitement):
    """
    Fonction retournant l'engine de connexion à la base de données.

    param user: user
    param pswd: mot de passe
    param host: hôte
    param dbase: nom de la base de données

    """
    connection = f'postgresql://{user}:{pswd}@{host}:{port}/{dbase}'
    return create_engine(connection)

# %%
def updateTable(new_lines, table_name, engine, schema, methode='append', geom=True, dtype=None, geometry_type='POLYGON', index_label='hex_id', chunksize=None):
    """
    ##todo: remplacer to_sql par to_postgis

    Fonction d'intégration ou de mise à jour des données dans le DWH.

    param new_lines: (geo)DataFrame contenant les données à intégrer
    param table_name: nom de la table de destination 
        (en mode 'append' si la table est inexistante ou en mode 'replace', une nouvelle table sera créee)
    param engine: engine de connexion à la base de données
    param schema: schéma dans lequel se trouve la table de destination
    param methode: 'append' pour ajouter des données à une table déjà existante ou 
        'replace' pour écraser la table de destination si elle existe déjà
    param geom: Boolean indiquant si les données contiennent une dimension géométrique
    param dtype: dictionnaire {nom_champ:type} permettant d'indique le type de certains champs
    param geometry_type: type de géométrie dans le cas d'un GeoDataFrame
    param index_label: nom de la colonne à passer en index
    param chunksize: nombre d'objets persistés simulatanément dans la base de données

    return: (Geo)DataDrame qui a été persisté dans le base de données

    """
    dict_types = {'geometry': Geometry(geometry_type, srid=3163)}
    
    if(methode=='replace'): # Suppression de la table de destination si elle existe déjà
        engine.execute(f'DROP TABLE IF EXISTS {schema}.{table_name} CASCADE')
    if(dtype is not None): # Mise à jour du dictionnaire des types de champs
        dict_types.update(dtype)
        
    if(not new_lines.empty): # Si des données sont à integrer
        if(geom): # Si les données contiennent une dimension géométrique
            if(new_lines.geometry.name != 'geom'):
                new_lines = new_lines.rename_geometry('geom')
            new_lines['geometry'] = new_lines['geom'].apply(lambda x: WKTElement(x.wkt, srid=3163))
            new_lines.drop('geom', 1, inplace=True)
            new_lines.to_sql(name=table_name, con=engine, schema=schema, if_exists=methode, index=True, index_label=index_label, dtype=dict_types, chunksize=chunksize)
        else:
            new_lines.to_sql(name=table_name, con=engine, schema=schema, if_exists=methode, index=True, index_label=index_label, dtype=dict_types, chunksize=chunksize)
    return new_lines

# %%
def geomView(table_name, engine, schema):
    """
    Génération d'une vue ajoutant une colonne de géométrie à une table donnée en argument
    qui possède une colonne d'identifiant hexagonal "hex_id".

    param table_name: nom de la table
    param engine: engine de connexion à la base de données
    param schema: schéma

    """
    query = f'DROP VIEW IF EXISTS {schema}.view_{table_name};' + f'CREATE VIEW {schema}.view_{table_name} AS (SELECT row_number() OVER() AS id, *, h3_to_geo_boundary(hex_id::h3index)::geometry AS geometry FROM {schema}.{table_name})'
    engine.execute(query)

# %% [markdown]
# ## Fonctions utiles

# %%
def loadData(catalog, table_name):
    """
    Fonction permettant de charger une table sous forme de DataFrame à partir 
    d'un catalogue Intake.

    param catalog: catalogue intake
    param table_name: nom de la table référencée dans le catalogue

    return: DataFrame

    """
    dataName = f"{table_name}"
    entryCatalog = getattr(open_catalog(catalog),dataName)
    data = entryCatalog
    return data.read()

# %%
def standardizeField(df, df_right, std_field_right, dic):
    """
    Fonction permettant de remplacer les valeurs d'un ou plusieurs champ(s) 
    d'une table par les valeurs d'un champ d'une autre table (appelé champ 
    de standardisation) suivant une jointure définie.

    param df: DataFrame en entrée
    df_right: DataFrame de jointure indexé sur son champ de jointure
    std_field_right: champ de standardisation de field_right
    dic: {join_field_df: champ de jointure de df, num_col: numéro de colonne du futur champ standardisé}

    return: DataFrame standardisé

    """
    for join_field_df, num_col in dic.items():
        std_field_df = df.join(df_right, on=join_field_df)[std_field_right] # Création du champ standardisé
        df = df.drop(join_field_df, axis= 1) # Suppression du champ non standardisé
        df.insert(num_col, join_field_df, std_field_df, allow_duplicates=True) # Insertion du champ standardisé
    return df

# %%
def compute_dict_tx(min_carto_unit_m):
    """
    Fonction retourant un dictionnaire associant à chaque résolution le taux à partir duquel un 
    élément classifiant est considéré comme unique dans chaque cellule.

    param min_carto_unit_m: unité minimale de cartographie (plus petit détail visible) en m
    return: dictionnaire

    """
    dic = {}
    # Pour chaque échelle de résolution H3
    for i in range (16):
        val = 1-(((min_carto_unit_m**2)/1000000)/hex_area[i])
        if val >= 0:
            dic[i] = val
        else:
            dic[i] = 0
    return dic

# %%
def h3_polyfill(gdf, resolution):
    """
    Fonction adaptée de h3pandas.polyfill. 
    Fonction ajoutant une colonne contenant les idenifiants des hexagones relatifs à 
    l'objet concerné pour la résolution donnée en argument.

    param gdf: GeoDataFrame en entrée
    param resolution: résolution des hexagones

    return: GeoDataFrame

    """
    def func(row):
        return list(shapely.polyfill(row.geometry, resolution, True))
    result = gdf.apply(func, axis=1) # Application de la fonction à chaque ligne
    assign_args = {"hex_id": result}
    return gdf.assign(**assign_args)

# %%
def segmentation(gdf, resolution):
    """
    Fonction réalisant une segmentation hexagonale du gdf en argument 
    à la résolution demandée.

    param gdf: GeoDataFrame en entrée
    param resolution: résolution des hexagones

    return: GeoDataFrame composé des colonnes "hex_id" et "geometry"

    """
    # Application d'un buffer de la taille d'un demi-hexagone
    len_buffer = np.sqrt(hex_area[resolution]*1000000)*0.6 # 60% de la largeur d'un hexagone de granularité=resolution
    gdf_init = gpd.GeoSeries(gdf.geometry.unary_union.buffer(len_buffer), crs="EPSG:3163")
    return h3fy(gdf_init,resolution)

# %%
def to_children(gdf, resolution=None):
    """
    Fonction retournant la liste des enfants (et leur géométrie) des hexagones 
    stockés dans le GeoDataFrame en entrée.

    param gdf: GeoDataFrame en entrée ("hex_id" doit être en index)
    param resolution: résolution des hexagones enfants
        Si resolution=None les enfants seront les enfants directs des objets du gdf en argument
        (sans saut hiérarchique).

    return: GeoDataFrame

    """
    list_index = []
    list_coord = []
    # Pour chaque hexagone en entrée
    for index, row in gdf.iterrows():
        id_children = h3.h3_to_children(index,resolution) # Index des enfants
        list_index += list(id_children)
    list_geom = [h3.h3_to_geo_boundary(index) for index in list_index] # Géométrie des enfants

    # Inversion des coordonnées
    for elem in list_geom:
        coord = []
        for point in elem:
            point = tuple(reversed(point))
            coord.append(point)
        list_coord.append(tuple(coord))

    list_geom = [shapely.Polygon(elem) for elem in list_coord] # Conversion des coordonnées en type Polygon
    output = gpd.GeoDataFrame({'geometry': list_geom}, geometry='geometry', crs='EPSG:4326', index=list_index)
    output.index.name = "hex_id"
    return output.to_crs(3163)

# %%
def area_tables(source_df, target_df):
    """
    Fonction adaptée de tobler.area_interpolate.

    """
    # Il est en général plus performant d'utiliser le plus long DataFrame comme index spatial
    if source_df.shape[0] > target_df.shape[0]:
        spatial_index = "source"
    else:
        spatial_index = "target"

    # Index de liaison entre la source et la target par intersection
    if spatial_index == "source":
        ids_tgt, ids_src = source_df.sindex.query_bulk(target_df.geometry, predicate="intersects")
    elif spatial_index == "target":
        ids_src, ids_tgt = target_df.sindex.query_bulk(source_df.geometry, predicate="intersects")

    # Liste des aires d'intersection
    areas = source_df.geometry.values[ids_src].intersection(target_df.geometry.values[ids_tgt]).area

    # Co-matrice des aires
    table = coo_matrix((areas,(ids_src, ids_tgt)), shape=(source_df.shape[0], target_df.shape[0]), dtype=np.float32)
    table = table.tocsr()

    return table

# %%
def h3_area_interpolate(source_df, target_df, categorical_variables):
    """
    Fonction adaptée de tobler.area_interpolate.

    """
    table = area_tables(source_df, target_df)
    for variable in categorical_variables: # Pour chaque champ de classe
        unique = source_df[variable].unique() # Liste des classes d'un champ
        for value in unique: # Pour chaque classe
            mask = source_df[variable] == value # Dataframe composé uniquement de la même classe
            target_df[value] = pd.DataFrame(np.asarray(table[mask].sum(axis=0))[0]).div(target_df.area.values, axis="rows")

    target_df.set_index("hex_id", inplace=True, drop=True)
    return target_df

# %% [markdown]
# ### Dask

# %%
def compact_dask_fct(gdf, npartitions, tx_spatial, res_min, res_max, fast=False):
    """
    Fonction indexant un GeoDataFrame sur une grille dynamique avec superpositions possibles d'objets
    en parallélisant les tâches à l'aide de l'outil Dask.

    param gdf: GeoDataFrame en entrée
    param npartitions: nombre de partitions utilisé par Dask pour la parralélisation
    param tx_spatial: taux minimal de remplissage d'une zone de données pour chaque échelle de résolution
        (sous forme de dictionnaire)
    param res_min: résolution minimale des hexagones
    param res_max: résolution maximale des hexagones
    param fast: Si fast=False, l'algorithme est plus lent mais l'indexation se fait au plus juste.
        Si fast=True, l'algorithme est rapide mais certaines périphéries d'objets risquent de ne pas être indexées.
    
    return: DataFrame

    """
    # Structure du DataFrame renvoyé en sortie
    # On ne conserve pas la colonne géométrie
    df_meta = pd.DataFrame(columns=gdf.drop(columns=[gdf.geometry.name]).columns.tolist())
    df_meta.index.names = ['hex_id']
    
    data = ddg.from_geopandas(gdf,npartitions)
    gdf_map = data.map_partitions(func=compact, tx_spatial=tx_spatial, res_min=res_min, res_max=res_max, fast=fast, meta=df_meta)
    client.persist(gdf_map)
    return gdf_map.compute()

# %%
def compact_dask_partition_fct(gdf, decoupage, npartitions, colonne, tx, res_min, res_max, tx_spatial=0.5):
    """
    Fonction indexant un GeoDataFrame sur une grille adaptative en parallélisant les tâches à l'aide de l'outil Dask.
    Attention, le géodataframe en entrée doit correspondre à une partition de l'espace.

    param gdf: GeoDataFrame en entrée
    param decoupage: GeoDataFrame composé des identifiants et de la géométrie des hexagones de plus faible résolution
        (= maillage hexagonal grossier de la donnée en entrée). Ce GeoDataFrame peut être obtenu via la fonction "segmentation".
    param npartitions: nombre de partitions utilisé par Dask pour la parralélisation
    param colonne: nom du champ à conserver à la fin de l'indexation
    param tx: taux minimal de remplissage d'une zone de données pour chaque échelle de résolution
        (sous forme de dictionnaire)
    param res_min: résolution minimale des hexagones
    param res_max: résolution maximale des hexagones
    param tx_spatial: taux de remplissage minimal de conservation d'un hexagone 
        (uniquement lors de la dernière itération de l'algorithme i.e. res=res_max)

    return: GeoDataFrame

    """   
    gdf_invalid = decoupage
    # Fonction utilisée lors de la parallélisation
    def my_fct(geom_clip, gdf, colonne, tx, res_min, res_max, tx_spatial):
        clip = to_children(geom_clip, res_min)
        gdf_valid_bdd = compact_for_dask_use(clip=clip, gdf=gpd.clip(gdf, clip, keep_geom_type=True), colonne=colonne, tx=tx, res_min=res_min, res_max=res_max, tx_spatial=tx_spatial)
        return gdf_valid_bdd

    # Création d'un GeoDataFrame vide
    output = gpd.GeoDataFrame(columns=[colonne,'geometry'], geometry='geometry')
    output.index.names = ['hex_id']
    
    # Structure du DataFrame renvoyé en sortie
    df_meta = pd.DataFrame(columns=[colonne,'geometry','type'])
    df_meta.index.names = ['hex_id']

    while(res_min <= res_max):
        print('   Résolution ' +  str(res_min))

        # return my_fct(gdf_invalid, gdf=gdf, colonne=colonne, tx=tx, res_min=res_min, res_max=res_max)
        data = ddg.from_geopandas(gdf_invalid, npartitions)
        gdf_map = data.map_partitions(func=my_fct, gdf=gdf, colonne=colonne, tx=tx, res_min=res_min, res_max=res_max, tx_spatial=tx_spatial, meta=df_meta)
        client.persist(gdf_map)
        gdf_bdd = gdf_map.compute()
        
        gdf_valid = gdf_bdd[gdf_bdd['type']=='valid'].drop(columns=['type'])
        gdf_invalid = gdf_bdd[gdf_bdd['type']=='invalid'].drop(columns=['type',colonne])

        # Concaténation avec les cellules valides de la résolution précédente
        output = pd.concat([output, gdf_valid], ignore_index=False) 
        # Incrémentation de la résolution
        res_min+=1
    output.drop('geometry', axis=1, inplace=True) # Suppression de la colonne des géométries
    return output

# %%
def compact_for_dask_use(clip, gdf, colonne, tx, res_min, res_max, tx_spatial=0.5):
    """
    Fonction indexant un GeoDataFrame sur une grille adaptative.
    Attention, le géodataframe en entrée doit correspondre à une partition de l'espace.

    param clip: GeoDataFrame composé des identifiants et de la géométrie des hexagones correspondant au gdf en argument
        (= maillage hexagonal de la donnée en entrée).
    param gdf: GeoDataFrame en entrée
    param colonne: nom du champ à conserver à la fin de l'indexation
    param tx: taux minimal de remplissage d'une zone de données pour chaque échelle de résolution
        (sous forme de dictionnaire)
    param res_min: résolution minimale des hexagones
    param res_max: résolution maximale des hexagones
    param tx_spatial: taux de remplissage minimal de conservation d'un hexagone 
        (uniquement lors de la dernière itération de l'algorithme i.e. res=res_max)

    return: GeoDataFrame

    """
    valid_cells = [] # Cellules n'ayant pas besoin d'être divisées
    valid_label = [] # Classe associée à chaque cellule
    list_geom_valid = []
    invalid_cells=[] # Cellules ayant besoin d'être divisées
    list_geom_invalid = []

    # Association des taux d'occupation de chaque classe pour chaque hexagone
    gdf_interp = h3_area_interpolate(source_df=gdf, target_df=clip.reset_index(), categorical_variables=[colonne])

    # Pour chaque enregistrement
    if len(gdf_interp.columns[1:]):
        for index, row in gdf_interp.iterrows():
            maximum = max(row[gdf_interp.columns[1:]])
            somme = row[gdf_interp.columns][1:].sum()
            end=False

            if somme < 1-tx[res_min] and (res_min < res_max):
                end = True
            
            else:
                for i in range(1,gdf_interp.columns.size): # Pour chaque colonne (sauf la géométrie)
                    valeur_i = row[gdf_interp.columns[i]]

                    if res_min < res_max: # Si la résolution maximale n'est pas atteinte
                        # Si le pourcentage de présence de la classe dans la cellule est supérieure à tx_spatial et est supérieure à tx de l'ensemble des classes
                        if valeur_i >= tx[res_min]:
                            valid_cells.append(index)
                            valid_label.append(gdf_interp.columns[i])
                            list_geom_valid.append(row['geometry'])
                            end = True
                            break
                    else: # Si la résolution maximale est atteinte
                        # La valeur de cellule est déterminée par la classe majoritaire même si tx n'est pas atteint
                        if(valeur_i == maximum) and (somme >= tx_spatial):
                            valid_cells.append(index)
                            valid_label.append(gdf_interp.columns[i])
                            list_geom_valid.append(row['geometry'])
                            end = True
                            break
            if (not end) and (res_min < res_max):
                invalid_cells.append(index)
                list_geom_invalid.append(row['geometry'])

        # Création d'un GeoDataFrame contenant les cellules valides avec leur classe associée
        gdf_valid_bdd = gpd.GeoDataFrame({colonne: valid_label, 'geometry': list_geom_valid, 'type':'valid'}, geometry='geometry', crs="EPSG:3163", index=valid_cells)
        gdf_valid_bdd.index.name = "hex_id"
        # Création d'un GeoDataFrame contenant les cellules invalides
        gdf_invalid_bdd = gpd.GeoDataFrame({colonne: None, 'geometry': list_geom_invalid, 'type':'invalid'}, geometry='geometry', crs="EPSG:3163", index=invalid_cells)
        gdf_invalid_bdd.index.name = "hex_id"
        return pd.concat([gdf_valid_bdd, gdf_invalid_bdd], ignore_index=False) 
    else:
        return gpd.GeoDataFrame({colonne: None, 'geometry': clip['geometry'], 'type':'invalid'}, geometry='geometry', crs="EPSG:3163", index=clip.index)

# %% [markdown]
# # Données

# %% [markdown]
# ## Récupération des données sources

# %%
# Connexion à la base de données "oeil_traitement"
engine = getEngine()

# %%
# Connexion à la base de données du RDS
engineRDS = getEngine(user='postgres',pswd='XwUxFfrL6yRK5Wz',host='oeil-pg-aws.cluster-ck8dgtf46vxd.ap-southeast-2.rds.amazonaws.com',dbase='oeil')

# %%
catalog = f"{data_catalog_dir}bilbo_data.yaml" # Choix du catalogue de données

# %%
# Coordonnées d'une bbox sur l'île des Pins
xmin = 536159
xmax = 571819
ymin = 156515
ymax = 190058

# %% [markdown]
# ## Tables de dimensions

# %% [markdown]
# ### communes

# %%
# %%time
# Récupétation de la table des communes (sur l'emprise souhaitée)
data_communes = loadData(catalog,'communes') # .cx[xmin:xmax, ymin:ymax]

# %%
data_communes # Visualisation de la table

# %% [markdown]
# ### provinces

# %%
# %%time
# Récupétation de la table des provinces (sur l'emprise souhaitée)
data_provinces = loadData(catalog,'provinces') # .cx[xmin:xmax, ymin:ymax]

# %%
data_provinces # Visualisation de la table

# %% [markdown]
# ### dim_dates

# %%
# %%time
# Récupétation de la table de standardisation des dates
# Le set_index sur le champ de jointure est nécessaire à l'application de la fonction "standardizeField"
data_date = pd.read_sql("SELECT * FROM pression_eau.dim_date",engine).set_index('date')

# %%
data_date # Visualisation de la table

# %% [markdown]
# ## Tables de faits

# %% [markdown]
# ### incendies_Sentinel

# %%
# %%time
# Récupétation de la table des incendies sur l'emprise souhaitée
data_feux_raw = loadData(catalog,'incendies_Sentinel') #.cx[xmin:xmax, ymin:ymax]

# %%
data_feux_raw # Visualisation de la table

# %%
# Standardisation des champs de dates
data_feux = standardizeField(data_feux_raw, data_date, 'date_id', {'begdate':5, 'enddate':6, 'derniere_detection':21}) #.cx[xmin:xmax, ymin:ymax]

# %%
data_feux # Visualisation de la table

# %% [markdown]
# ### mos_2014

# %%
# %%time
# Récupétation de la table de MOS de 2014 sur l'emprise souhaitée
data_mos2014 = loadData(catalog,'mos2014') #.cx[xmin:xmax, ymin:ymax]

# %%
data_mos2014 # Visualisation de la table

# %%
# %%time
# Récupétation de la table de MOS de 2014 sur l'île des Pins
data_mos2014_idp = data_mos2014.cx[xmin:xmax, ymin:ymax]

# %% [markdown]
# # Indexation des données

# %% [markdown]
# ## Maillage simple

# %% [markdown]
# ### communes

# %%
# %%time
# Maillage hexagonal des communes
segmentation_communes = segmentation(data_communes,8)

# %%
# %%time
compact_dask_partition(tab_name="test", schema="bilbo", gdf=data_communes, decoupage=segmentation_communes, nb_cluster=10, npartitions=16, colonne='nom', tx=compute_dict_tx(7), res_min=8, res_max=8, i_start=1, tx_spatial=0)

# %% [markdown]
# ### provinces

# %%
# %%time
# Maillage hexagonal des provinces
segmentation_provinces = segmentation(data_provinces,8)

# %%
# %%time
compact_dask_partition(tab_name="test", schema="bilbo", gdf=data_provinces, decoupage=segmentation_provinces, nb_cluster=10, npartitions=16, colonne='nom', tx=compute_dict_tx(7), res_min=8, res_max=8, i_start=1, tx_spatial=0)

# %% [markdown]
# ## Maillage adaptatif

# %% [markdown]
# ### incendies_Sentinel

# %%
# %%time
compact_dask(tab_name="test", schema="bilbo", gdf=data_feux, tx_spatial=compute_dict_tx(7), res_min=6, res_max=13, nb_cluster=2, i_start=1, fast=False)

# %% [markdown]
# ### mos_2014

# %%
# %%time
# Maillage hexagonal du mos2014
segmentation_mos2014 = segmentation(data_mos2014,6)

# %%
# %%time
compact_dask_partition(tab_name="test", schema="bilbo", gdf=data_mos2014, decoupage=segmentation_mos2014, nb_cluster=200, npartitions=16, colonne='l_2014_n3', tx=compute_dict_tx(7), res_min=6, res_max=13, i_start=1)

# %%
# %%time
# Maillage hexagonal du mos2014 sur l'île des Pins
segmentation_mos2014_idp = segmentation(data_mos2014_idp,6)

# %%
# %%time
compact_dask_partition(tab_name="test", schema="bilbo", gdf=data_mos2014_idp, decoupage=segmentation_mos2014_idp, nb_cluster=3, npartitions=16, colonne='l_2014_n3', tx=compute_dict_tx(7), res_min=6, res_max=13, i_start=1)

# %% [markdown]
# ## Visualisation

# %%
# Création d'une vue avec ajout de la colonne géométrie
geomView("test", engine, "bilbo")


