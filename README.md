# ProQuest Record Linkage

The current version of the record linkage script is `link3.py`, the last known working version is `link2.py` The main difference between these two files is that the functions in `link2.py` would be called individually from a python command prompt, and `link3.py` is meant to be called from the command-line with different flags to perform different functions. The rest of this README will describe `link3.py`, but `link2.py` is kept in this repository for reference.

The script performs the following tasks:

1. Creates summary tables of STAR METRICS and ProQuest data.
2. Creates input files for record linkage
3. Calls the external record linkage program
4. Calls the extrernal 1-to-1 assignment program
5. Saves the extracted links to the database

Run the script by execute `python link3.py` at the command line with command-line flags to indicate which functions to perform. For example, the follow call will initialize the STAR METRICS and ProQuest summary tables:

    python link3.py --sm-init --pq-init

## Configuration file

The script looks for a file named `config.properties`. An example is provided in the repository.

## Command-line flags

<table>

<tr>
<td><code>--table-prefix</code></td>
<td>The table prefix to use for all tables created by this script.</td>
</tr>

<tr>
<td><code>--directory</code></td>
<td>The directory to use for record linkage input and output files.</td>
</tr>

<tr>
<td><code>--life-science-init</code></td>
<td>Initialize the lookup table of ProQuest life science codes.</td>
</tr>

<tr>
<td><code>--gender-probabilities-init</code></td>
<td>Initialize the lookup table of names genders.</td>
</tr>

<tr>
<td><code>--sm-init</code></td>
<td>Initialize the STAR METRICS summary tables.</td>
</tr>

<tr>
<td><code>--pq-init</code></td>
<td>Initialize the ProQuest summary tables</td>
</tr>

<tr>
<td><code>--matching-input</code></td>
<td>Create input files for record linkage. Produces the input files `smnames\_all.csv`, `smnames_grad.csv`, and `proquest.csv`.</td>
</tr>

<tr>
<td><code>--matching</code></td>
<td>Perform record linkage. Produces the (poorly named) output files `sm_pq_matching_output.csv` and `pq_sm_matching_output.csv`. The first file matches graduate students to ProQuest files, the second file matches all STAR METRICS employees to matching files. Before running the next step, you should use these files to create `sm_pq_matching_output_clerical.csv` and `pq_sm_matching_output_clerical.csv` by deleting all record comparisons with match scores below the desired cutoff threshold.</td>
</tr>

<tr>
<td><code>--assignment</code></td>
<td>Perform 1-to-1 link extraction. This read in the files `sm_pq_matching_output_clerical.csv` and `pq_sm_matching_output_clerical.csv` and produce the files `sm_pq_links_1x1.csv` and `pq_sm_links_1x1.csv`.</td>
</tr>

<tr>
<td><code>--upload</code></td>
<td>Upload links to the database.</td>
</tr>

## Record linkage program

The programs `Match.java` and `Assign.java` in the `Library` folder will need to be compiled in order to run the match step. You will need the record linkage jar file file in your class path in order to compile.

## Input files

You will need two input files:

* The "life sciences" file, which provides a list of ProQuest subject codes that should be flagged as belonging to the life sciences
* the `ssnnamesout.dat` file, which is the output file from the name/gender prediction paper.

</table>
