#Adopted from RDatasets

module Datasets

using Reexport
@reexport using DataFrames
@reexport using DataFrameSets

export dataset

#Load a particular dataframe
function dataset(package_name::AbstractString, dataset_name::AbstractString)
    basename = joinpath(dirname(@__FILE__), "..", "data", package_name)

    filename = joinpath(basename, string(dataset_name, ".csv.gz"))
    if !isfile(filename)
        error(@sprintf "Unable to locate file %s\n" filename)
    else
        return readtable(filename)
    end
end

#Load all dataframes in the package, returns a DFSet
function dataset(package_name::AbstractString; getname::Function=fileroot)
  dir = joinpath(dirname(@__FILE__), "..", "data", package_name)
  files = readdir(dir)

  dfs = DFSet()
  for f in files
    D = readtable(joinpath(dir, f))
    name = getname(f, D)
    push!(dfs, (name, D))
  end
  return dfs
end

fileroot(filename::AbstractString, D::DataFrame) = split(basename(filename), '.')[1]

end # module
