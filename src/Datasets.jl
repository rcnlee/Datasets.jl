#Adopted from RDatasets

module Datasets

using Reexport
@reexport using DataFrames
using RLESUtils
@reexport using DataFrameSets

export dataset, listdatasets, listdataset

const DATAPATH = joinpath(dirname(@__FILE__), "..", "data")

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

function listdatasets()
    dsets = readdir(DATAPATH)
    filter!(x -> isdir(joinpath(DATAPATH, x)), dsets)
    dsets
end

function listdataset(package_name::AbstractString)
   data = readdir(joinpath(DATAPATH, package_name))
   filter!(x -> endswith(x, ".csv.gz"), data)
   data
end

end # module
