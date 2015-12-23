#Excerpted from RDatasets

module Datasets

using Reexport
@reexport using DataFrames

export dataset

function dataset(package_name::AbstractString, dataset_name::AbstractString)
    basename = joinpath(dirname(@__FILE__), "..", "data", package_name)

    filename = joinpath(basename, string(dataset_name, ".csv.gz"))
    if !isfile(filename)
        error(@sprintf "Unable to locate file %s\n" filename)
    else
        return readtable(filename)
    end
end

end # module
