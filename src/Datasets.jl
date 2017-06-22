#Adopted from RDatasets

module Datasets

using Reexport
@reexport using DataFrames
using RLESUtils
@reexport using DataFrameSets

export dataset, list_datasets, list_dataset, datadir, load_meta, write_dataset

const DATAPATH = joinpath(dirname(@__FILE__), "..", "data")
const METAFILE = "_META.csv.gz"

#Load a particular dataframe
function dataset(package_name::AbstractString, dataset_name::AbstractString)
    dirpath = joinpath(DATAPATH, package_name)

    filename = joinpath(dirpath, string(dataset_name, ".csv.gz"))
    if !isfile(filename)
        error(@sprintf "Unable to locate file %s\n" filename)
    else
        return readtable(filename)
    end
end

#Load all dataframes in the package, returns a DFSet
function dataset(data_name::AbstractString)
    dirpath  = joinpath(DATAPATH, data_name)
    if !isdir(dirpath)
        error(@sprintf "No such directory %s\n" dirpath)
    end
    Ds = load_csvs(dirpath)
    Ds
end

#Load all dataframes in the package, returns a DFSet
function dataset(data_name::AbstractString, label::Symbol; 
    transform::Function=identity)
    Ds = dataset(data_name)
    Dl = DFSetLabeled(Ds, label; transform=transform)
    Dl
end

function list_datasets()
    dsets = readdir(DATAPATH)
    filter!(x -> isdir(joinpath(DATAPATH, x)), dsets)
    dsets
end

function list_dataset(package_name::AbstractString)
   data = readdir(joinpath(DATAPATH, package_name))
   filter!(x -> endswith(x, ".csv.gz"), data)
   data
end

datadir(package_name::AbstractString="") = abspath(joinpath(DATAPATH, package_name))

function load_meta(data_name::AbstractString)
    dirpath  = joinpath(DATAPATH, data_name)
    if !isdir(dirpath)
        error(@sprintf "No such directory %s\n" dirpath)
    end
    M = readtable(joinpath(dirpath, METAFILE))
    M 
end

function write_dataset(data_name::AbstractString, Ds::DFSet)
    dirpath  = joinpath(DATAPATH, data_name)
    m = getmeta(Ds)
    m[:id] = 1:nrow(m)
    save_csvs(dirpath, Ds) 
end

end # module
