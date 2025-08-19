# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/header_gold_notebook

# COMMAND ----------

# database_name = "g_upi" # [str]
# incremental = False # [bool]
# metadata_container = "datalake" # [str]
# metadata_folder = "/datalake/g_upi/metadata" # [str]
# metadata_storage = "edmans{env}data003" # [str]
# overwrite = False # [bool]
# prune_days = 10 # [int]
# sampling = False # [bool]
# table_name = "UPI_PIM_PRODUCT_STAGING" # [str]
# target_container = "datalake" # [str]
# target_folder = "/datalake/g_upi/full_data" # [str]
# target_storage = "edmans{env}data003" # [str]

# COMMAND ----------

# VARIABLES
table_name = get_table_name(database_name, table_name)

# COMMAND ----------

# drop_table('g_upi._map_upi_pim_product_staging')
# drop_table('g_upi.upi_pim_product_staging')

# COMMAND ----------

# EXTRACT
source_table = 'pim.products'
if incremental:
  cutoff_value = get_cutoff_value(table_name, source_table, prune_days)
  pim = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  pim = load_full_dataset(source_table)

pim.createOrReplaceTempView('pim')

# COMMAND ----------

# SAMPLING
if sampling:
  pim = pim.limit(10)
  pim.createOrReplaceTempView('pim')

# COMMAND ----------

main_1 = spark.sql("""
select
  MdmId Internal_Item_Id,
  replace(Type, 'Api', '') Template_Name,
  Brand Brand,
  "PIM" _SOURCE,
  inline(
    arrays_zip(
      array(
        'AbrasionResistance',
        'Accessories',
        'AllergyPrevention',
        'BladeCutResistance',
        'CleanroomClass',
        'CuffLength',
        'CuffThickness',
        'DesignFeatures',
        'DisposalRecommendations',
        'DoubleGlovingRecommendation',
        'ExternalGloveSurface',
        'FingerThickness',
        'FreedomFromHoles',
        'GloveLength',
        'GripLevel',
        'InternalGloveSurface',
        'MedicalRegistrationNumber',
        'NotMadeFromNaturalRubberLatex',
        'PalmThickness',
        'PalmWidth',
        'PortSizes',
        'PowderContent',
        'PrimarySkinIrritation',
        'ProductCertification',
        'ProductFamily',
        'ProductReference',
        'ProductSegmentation',
        'ProteinLevel',
        'PunctureResistance',
        'SeamType',
        'SkinSensitization',
        'SpecificNeeds',
        'SterilizationMethod',
        'TearResistence',
        'TestedForUseWithChemotherapyDrugs',
        'ViralPenetration',
        'ProductMaterial',
        'ProductColour',
        'Thickness',
        'TieClosureType',
        'LinerColour',
        'LinerMaterial',
        'TypeOfProtection',
        'LevelOfProtection',
        'OldProductName',
        'Brand',
        'ProductName',
        'ProductType',
        'FeaturedTechnology',
        'CoatingMaterial',
        'CoatingColour',
        'Antistatic',
        'IsChlorinated',
        'SiliconeFree',
        'PossibleSensitizerIngredients',
        'Construction',
        'Hazardous',
        'GripDesign',
        'IsLatexFree',
        'CuffStyle',
        'Length',
        'OuterFacingLayer',
        'NoseBand',
        'InnerFacingLayer',
        'CoverTape',
        'LatexFree',
        'BindingTapes',
        'InnerLayer',
        'FiltrationLayer',
        'Weight',
        'Colour',
        'Reusable',
        'Shape',
        'Easybreath',
        'WashingTemperature',
        'FoodApproved',
        'ImpactTest',
        'Gauge',
        'Finishing',
        'Material',
        'Sterile',
        'Height',
        'PrimaryImage',
        'SecondaryImages',
        'Regions',
        'Excludes'
      ),
      array(
        AbrasionResistance,
        Accessories,
        AllergyPrevention,
        BladeCutResistance,
        CleanroomClass,
        CuffLength,
        CuffThickness,
        DesignFeatures,
        DisposalRecommendations,
        DoubleGlovingRecommendation,
        ExternalGloveSurface,
        FingerThickness,
        FreedomFromHoles,
        GloveLength,
        GripLevel,
        InternalGloveSurface,
        MedicalRegistrationNumber,
        NotMadeFromNaturalRubberLatex,
        PalmThickness,
        PalmWidth,
        PortSizes,
        PowderContent,
        PrimarySkinIrritation,
        ProductCertification,
        ProductFamily,
        ProductReference,
        ProductSegmentation,
        ProteinLevel,
        PunctureResistance,
        SeamType,
        SkinSensitization,
        SpecificNeeds,
        SterilizationMethod,
        TearResistence,
        TestedForUseWithChemotherapyDrugs,
        ViralPenetration,
        ProductMaterial,
        ProductColour,
        Thickness,
        TieClosureType,
        LinerColour,
        LinerMaterial,
        TypeOfProtection,
        LevelOfProtection,
        OldProductName,
        Brand,
        ProductName,
        ProductType,
        FeaturedTechnology,
        CoatingMaterial,
        CoatingColour,
        Antistatic,
        CASE
          WHEN IsChlorinated is true THEN "Y"
          ELSE "N"
        END,
        SiliconeFree,
        PossibleSensitizerIngredients,
        Construction,
        Hazardous,
        GripDesign,
        CASE
          WHEN IsLatexFree is true THEN "Y"
          ELSE "N"
        END,
        CuffStyle,
        Length,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        Weight,
        Colour,
        Reusable,
        Shape,
        "",
        WashingTemperature,
        FoodApproved,
        ImpactTest,
        Gauge,
        Finishing,
        Material,
        Sterile,
        Height,
        PrimaryImage,
        SecondaryImages,
        "",
        ""
      )
    )
  ) AS (DL_Attr_Col_Name, Product_Information)
FROM
  pim.products;
""")
main_1.display()

# COMMAND ----------

# main_1.filter('Internal_Item_id = "817899" and DL_Attr_Col_Name = "caseNetWeight"').display()

# COMMAND ----------

# main_1.filter('Internal_Item_Id = "817899" and DL_Attr_Col_Name = "caseNetWeight"').display()

# COMMAND ----------

main_2 = (
  main_1
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  #.transform(attach_source_column('TAA'))
)

# COMMAND ----------

# MAP TABLE
nk_columns = ['Internal_Item_Id', '_SOURCE']

map_table = get_map_table_name(table_name)
if not table_exists(map_table):
  map_table_path = get_table_path(map_table, target_container, target_storage)
  create_map_table(map_table, map_table_path)
    
update_map_table(map_table, main_2, nk_columns)

# COMMAND ----------

main_f = (
  main_2
  .transform(attach_primary_key(nk_columns, map_table, '_ID'))
  .transform(sort_columns)
)

main_f.createOrReplaceTempView('main_f')
# main_f.display()

# COMMAND ----------

# LOAD
options = {
  'overwrite': overwrite, 
  'storage_name': target_storage, 
  'container_name': target_container
}
register_hive_table(main_f, table_name, target_folder, options)
merge_into_table(main_f, table_name, ['_ID'])

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(pim, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'pim')
  update_run_datetime(run_datetime, table_name, 'pim')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/footer_gold_notebook
