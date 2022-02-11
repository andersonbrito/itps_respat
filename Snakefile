# Wildcards setting
LOCATIONS = ["country", "region", "states", "locations"]
SAMPLES = ["FLUA", "FLUB", "VSR", "SC2", 'META', 'RINO', 'PARA', 'ADENO', 'BOCA', 'COVS', 'ENTERO', 'BAC']

ruleorder: test_results_go > combine_posneg_go > total_tests_go > posrate_go > demog_go


rule arguments:
	params:
		datadir = "data",
		rename_file = "data/rename_columns.xlsx",
		shapefile = "/Users/anderson/GLab Dropbox/Anderson Brito/codes/geoCodes/bra_adm_ibge_2020_shp/bra_admbnda_adm2_ibge_2020.shp",
		cache = "config/cache_coordinates.tsv",
		age_groups = "config/demo_bins.txt",
		geography = "config/tabela_municipio_macsaud_estado_regiao.tsv",
		population = "config/municipio_faixasetarias_ibgeTCU.tsv",
		index_column = "division_exposure",
		date_column = "date_testing",
		start_date = "2021-12-19",


arguments = rules.arguments.params


rule reshape:
	message:
		"""
		Combine tables with testing data
		"""
	input:
		rename = arguments.rename_file,
		correction = "data/fix_values.xlsx"
	params:
		datadir = arguments.datadir
	output:
		matrix = "results/combined_testdata1.tsv"
	shell:
		"""
		python3 scripts/reshape_respvir.py \
			--datadir {params.datadir} \
			--rename {input.rename} \
			--correction {input.correction} \
			--output {output.matrix}
		"""


rule agegroups:
	message:
		"""
		Add column with age groups
		"""
	input:
		metadata = rules.reshape.output.matrix,
		bins = arguments.age_groups,
	params:
		column = "age",
		group = "age_group",
		lowest = "0",
		highest = "200",
	output:
		matrix = "results/combined_testdata2.tsv",
	shell:
		"""
		python3 scripts/groupbyrange.py \
			--input {input.metadata} \
			--column {params.column} \
			--bins {input.bins} \
			--group {params.group} \
			--lowest {params.lowest} \
			--highest {params.highest} \
			--output {output.matrix}
		"""


rule geomatch:
	message:
		"""
		Match location names with geographic shapefile polygons
		"""
	input:
		input_file = rules.agegroups.output.matrix,
		cache = arguments.cache,
		shapefile = arguments.shapefile,
	params:
		geo_columns = "state, location",
		add_geo = "country:Brazil",
		lat = "lat",
		long = "long",
		check_match = "ADM2_PT",
		target = "ADM1_PT, ADM1_PCODE, ADM2_PT, ADM2_PCODE",
	output:
		matrix = "results/combined_testdata3.tsv"
	shell:
		"""
		python3 scripts/name2shape.py \
			--input {input.input_file} \
			--shapefile \"{input.shapefile}\" \
			--geo-columns \"{params.geo_columns}\" \
			--add-geo {params.add_geo} \
			--lat {params.lat} \
			--long {params.long} \
			--cache {input.cache} \
			--check-match {params.check_match} \
			--target \"{params.target}\" \
			--output {output.matrix}
		"""


rule geocols:
	message:
		"""
		Match location names with geographic shapefile polygons
		"""
	input:
		file =  rules.geomatch.output.matrix,
		shapefile = arguments.shapefile,
		newcols = arguments.geography,
	params:
		target = "region, DS_UF_SIGLA, CO_MACSAUD, DS_NOMEPAD_macsaud",
		index = "ADM2_PCODE",
		action = "add",
		mode = "columns"
	output:
		matrix = "results/combined_testdata.tsv"
	shell:
		"""
		python3 scripts/reformat_dataframe.py \
			--input1 {input.file} \
			--input2 {input.newcols} \
			--index {params.index} \
			--action {params.action} \
			--mode {params.mode} \
			--targets "{params.target}" \
			--output {output.matrix}
		"""

rule demog_go:
	input:
		expand("results/demography/matrix_{sample}_agegroups.tsv", sample=SAMPLES),

tests = {
"FLUA": "FLUA_test_result",
"FLUB": "FLUB_test_result",
"VSR": "VSR_test_result",
"SC2": "SC2_test_result",
"FLUA": "FLUA_test_result",
"FLUB": "FLUB_test_result",
"META": "META_test_result",
"VSR": "VSR_test_result",
"RINO": "RINO_test_result",
"PARA": "PARA_test_result",
"ADENO": "ADENO_test_result",
"BOCA": "BOCA_test_result",
"COVS": "COVS_test_result",
"SC2": "SC2_test_result",
"ENTERO": "ENTERO_test_result",
"BAC": "BAC_test_result"
}

def set_groups(spl):
	yvar = tests[spl] + ' epiweek'
	id_col = tests[spl]
	filter = "sex:F, sex:M, ~age_group:"
	add_col = "pathogen:" + spl + ", name:Brasil"
	return([yvar, id_col, filter, add_col])

rule demog:
	message:
		"""
		Aggregate ages per age group and sex
		"""
	input:
		metadata = rules.geocols.output.matrix,
		bins = arguments.age_groups,
	params:
		xvar = "age_group",
		format = "integer",
		yvar = lambda wildcards: set_groups(wildcards.sample)[0],
		unique_id = lambda wildcards: set_groups(wildcards.sample)[1],
		filters = lambda wildcards: set_groups(wildcards.sample)[2],
		start_date = arguments.start_date,
		id_col = lambda wildcards: set_groups(wildcards.sample)[3],
	output:
		age_matrix = "results/demography/matrix_{sample}_agegroups.tsv"
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.metadata} \
			--xvar {params.xvar} \
			--format {params.format} \
			--yvar {params.yvar} \
			--new-columns \"{params.id_col}\" \
			--filter "{params.filters}" \
			--unique-id {params.unique_id} \
			--start-date {params.start_date} \
			--output {output.age_matrix}
			
		echo {params.unique_id}
		sed -i '' 's/{params.unique_id}/test_result/' {output.age_matrix}
		"""




rule combine_demog:
	message:
		"""
		Combine deographic results
		"""
	input:
		path = "results/demography",
		population = arguments.population
	params:
		regex = "*_agegroups.tsv",
		filler = "0",
		
		index1 = "name pathogen test_result epiweek",
		index2 = "name",
		rate = "100000",
		filter = "test_result:Positive",
	output:
		merged = "results/demography/combined_matrix_agegroups.tsv",
		caserate = "results/demography/combined_matrix_agegroups_100k.tsv"
	shell:
		"""
		python3 scripts/multi_merger.py \
			--path {input.path} \
			--regex {params.regex} \
			--fillna {params.filler} \
			--output {output.merged}
		
		python3 scripts/normdata.py \
			--input1 {output.merged} \
			--input2 {input.population} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--rate {params.rate} \
			--filter {params.filter} \
			--output {output.caserate} \

		cp {output.merged} figures/pyramid
		cp {output.caserate} figures/pyramid
		"""






rule test_results_go:
	input:
		expand("results/{geo}/matrix_{sample}_{geo}_posneg.tsv", sample=SAMPLES, geo=LOCATIONS),

index_results = {
"country": ["country", "\'\'"],
"region": ["region", "\'\'"],
"states": ["ADM1_PCODE", "ADM1_PT DS_UF_SIGLA country"],
"locations": ["ADM2_PCODE", "ADM2_PT state DS_UF_SIGLA"]
}

def set_index_results(spl, loc):
	yvar = tests[spl] + ' ' + index_results[loc][0]
	index = index_results[loc][0]
	extra_cols = index_results[loc][1]
	filter = "~" + tests[spl] + ":Not tested"
	add_col = "pathogen:" + spl
	test_col = tests[spl]
	return([yvar, index, extra_cols, filter, add_col, test_col])

rule test_results:
	priority: 10
	message:
		"""
		Compile data of respiratory pathogens
		"""
	input:
		input_file = rules.geocols.output.matrix
	params:
		xvar = arguments.date_column,
		xtype = "time",
		format = "integer",
		
		yvar = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[0],
		index = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[1],
		extra_columns = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[2],
		filters = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[3],
		id_col = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[4],

		start_date = arguments.start_date,
		test_col = lambda wildcards: set_index_results(wildcards.sample, wildcards.geo)[5],
	output:
		"results/{geo}/matrix_{sample}_{geo}_posneg.tsv",
	shell:
		"""
		python3 scripts/rows2matrix.py \
			--input {input.input_file} \
			--xvar {params.xvar} \
			--xtype {params.xtype} \
			--format {params.format} \
			--yvar {params.yvar} \
			--unique-id {params.index} \
			--extra-columns {params.extra_columns} \
			--new-columns "{params.id_col}" \
			--filters "{params.filters}" \
			--start-date {params.start_date} \
			--output {output}
		
		sed -i '' 's/{params.test_col}/test_result/' {output}
		"""


rule combine_posneg_go:
	input:
		expand("results/{geo}", geo=LOCATIONS),
		expand("results/{geo}/combined_matrix_{geo}_posneg.tsv", geo=LOCATIONS),
		expand("results/{geo}/combined_matrix_{geo}_posneg_weeks.tsv", geo=LOCATIONS),

rule combine_posneg:
	message:
		"""
		Combine test results
		"""
	input:
		path = "results/{geo}"
	params:
		regex = "*_posneg.tsv",
		filler = "0",
		unit = "week",
		format = "integer"
	output:
		merged = "results/{geo}/combined_matrix_{geo}_posneg.tsv",
		merged_weeks = "results/{geo}/combined_matrix_{geo}_posneg_weeks.tsv"
	shell:
		"""
		python3 scripts/multi_merger.py \
			--path {input.path} \
			--regex {params.regex} \
			--fillna {params.filler} \
			--output {output.merged}
		
		python3 scripts/aggregator.py \
			--input {output.merged} \
			--unit {params.unit} \
			--format {params.format} \
			--output {output.merged_weeks}
		
		cp results/country/combined_matrix_country_posneg.tsv figures/barplot
		"""



rule total_tests_go:
	input:
		expand("results/{geo}/combined_matrix_{geo}_posneg.tsv", geo=LOCATIONS),
		expand("results/{geo}/combined_matrix_{geo}_totaltests.tsv", geo=LOCATIONS),

index_totals = {
"country": ["pathogen country", "country", "\'\'"],
"region": ["pathogen region", "region", "\'\'"],
"states": ["pathogen DS_UF_SIGLA", "DS_UF_SIGLA", "ADM1_PCODE ADM1_PT country"],
"locations": ["pathogen ADM2_PCODE", "ADM2_PCODE", "ADM2_PT state DS_UF_SIGLA"]
}

def set_index_totals(loc):
	index = index_totals[loc][0]
	unique_id = index_totals[loc][1]
	extra_cols = index_totals[loc][2]
	return([index, unique_id, extra_cols])


rule total_tests:
	message:
		"""
		Get total tests performed per pathogen
		"""
	input:
		file = "results/{geo}/combined_matrix_{geo}_posneg.tsv"
	params:
		format = "integer",
		index = lambda wildcards: set_index_totals(wildcards.geo)[0],
		unique_id = lambda wildcards: set_index_totals(wildcards.geo)[1],
		extra_columns = lambda wildcards: set_index_totals(wildcards.geo)[2],
		filters = "~test_result:Not tested",
	output:
		"results/{geo}/combined_matrix_{geo}_totaltests.tsv",
	shell:
		"""
		python3 scripts/collapser.py \
			--input {input.file} \
			--index {params.index} \
			--unique-id {params.unique_id} \
			--extra-columns {params.extra_columns} \
			--format {params.format} \
			--filter \"{params.filters}\" \
			--output {output}
		"""

rule posrate_go:
	input:
		expand("results/{geo}/combined_matrix_{geo}_posrate.tsv", geo=LOCATIONS)

indexes = {
"country": ["pathogen country"],
"region": ["pathogen region"],
"states": ["pathogen ADM1_PCODE DS_UF_SIGLA"],
"locations": ["pathogen ADM2_PCODE"]
}

def getIndex(loc):
	id = indexes[loc][0]
	return(id)

rule posrate:
	message:
		"""
		Test positive rates
		"""
	input:
		file1 = rules.combine_posneg.output.merged,
		file2 = rules.total_tests.output
	params:
		index1 = lambda wildcards: getIndex(wildcards.geo),
		index2 = lambda wildcards: getIndex(wildcards.geo),
		filter = 'test_result:Positive',
		min_denominator = 50
	output:
		"results/{geo}/combined_matrix_{geo}_posrate.tsv"
	shell:
		"""
		python3 scripts/normdata.py \
			--input1 {input.file1} \
			--input2 {input.file2} \
			--index1 {params.index1} \
			--index2 {params.index2} \
			--min-denominator {params.min_denominator} \
			--filter {params.filter} \
			--output {output}

		cp results/country/combined_matrix_country_posrate.tsv figures/lineplot
		"""



#rule xxx:
#	message:
#		"""
#		
#		"""
#	input:
#		metadata = arguments.
#	params:
#		index = arguments.,
#		date = arguments.
#	output:
#		matrix = "results/"
#	shell:
#		"""
#		python3 scripts/ \
#			--metadata {input.} \
#			--index-column {params.} \
#			--extra-columns {params.} \
#			--date-column {params.} \
#			--output {output.}
#		"""
#
#


rule clean:
	message: "Removing directories: {params}"
	params:
		"results"
	shell:
		"""
		rm -rfv {params}
		"""
