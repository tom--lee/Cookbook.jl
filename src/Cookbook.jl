
module Cookbook

using Distributed
using LightGraphs

export Recipe
export make

abstract type Recipe end

function _print_item(io::IO, item::String; level = 1)
    for i in 1:level+1
        print(io, "  ")
    end
    println(io, item)
end

function _print_item(io::IO, items; level = 1)
    for item in items
        _print_item(io, item, level = level + 1)
    end
end

function Base.show(io::IO, ::MIME"text/plain", x::Recipe)
    println(io, "Recipe for $(typeof(x))")
    println(io, "  Products:")
    for product in pairs(x.products)
        println(io, "  ")
        print(io, string( first(product) ,":\n"))
        _print_item(io, last(product))
    end
    println(io, "  Dependencies: ")
    for dep in pairs(x.deps)
        println(io, "  ")
        print(io, string( first(dep) ,":\n"))
        _print_item(io, last(dep))
    end
    for field in fieldnames(typeof(x))
        if field != :products && field != :deps
            println(io, "$(field) = $(getfield(x,field))")
        end
    end
    nothing
end

lastmodified(filename) = if isfile(filename)
    stat(filename).mtime
else
    Inf
end

is_stale(::Any; err = false) = true

function is_stale(recipe::Recipe, product::String, dep::String)
    if !ispath(dep)
        if ispath(product)
            @debug "Missing recipe dependency for extant product." recipe dep
            false
        else
            @error "Missing recipe dependency." recipe dep
            error("Missing dependency")
            true
        end
    elseif !ispath(product)
        @info "Recipe is stale due to missing product" recipe product
        true
    elseif lastmodified(product) < lastmodified(dep)
        @info "Recipe is stale due to new dependency" recipe dep
        true
    else
        false
    end
end

function is_stale(recipe::Recipe, product::String, deps)
    mapreduce(|, deps) do dep
        is_stale(recipe, product, dep)
    end
end
function is_stale(recipe::Recipe, products, dep::String)
    mapreduce(|, products) do product
        is_stale(recipe, product, dep)
    end
end

function is_stale(recipe::Recipe; err=false)
    for product in recipe.products, dep in recipe.deps
        if is_stale(recipe, product, dep)
            if err
                error("Recipe should not be stale")
            end
            return true
        end
    end
    false
end

function add_dep!(graph, product2recipe, product_idx::Int, dependency)
    if dependency isa String
        if haskey(product2recipe, dependency)
            j = product2recipe[dependency]
            add_edge!(graph, product_idx, j)
        elseif !ispath(dependency)
            error("Missing dependency:\n  $dependency")
        end
    else
        for dep in dependency
            if haskey(product2recipe, dep)
                j = product2recipe[dep]
                add_edge!(graph, product_idx, j)
            elseif !ispath(dep)
                error("Missing dependency:\n  $dep")
            end
        end
    end
    nothing
end

function make_graph(product2recipe, num_recipes, dependencies)
    graph = SimpleDiGraph(num_recipes)
    products = collect(keys(product2recipe))
    for product in products
        i = product2recipe[product]
        for dependency in dependencies[i]
            add_dep!(graph, product2recipe, i::Int, dependency)
        end
    end
    graph
end


#returns recipes that are not already in order and for which all the dependencies
#are already in `order`
function recipe_order(graph)
    order = []
    in_neighbors = map(x->inneighbors(graph,x), vertices(graph))
    out_neighbors = map(x->outneighbors(graph,x), vertices(graph))
    num_in_neighbors = map(x->length(in_neighbors[x]), vertices(graph))
    num_out_neighbors = map(x->length(out_neighbors[x]), vertices(graph))
    allocated = zeros(Bool, length(vertices(graph)))
    while !reduce( & , allocated )
        next = []
        new_allocated = zeros(Bool, length(vertices(graph)))
        for vertex in vertices(graph)
            if allocated[vertex]
                continue
            elseif num_out_neighbors[vertex] == 0 || reduce( & , allocated[out_neighbors[vertex]] )
                push!(next, vertex)
                new_allocated[vertex] = true
            end
        end
        allocated .|= new_allocated
        @assert length(next)>0
        append!(order, sort(
             next,
             by=x->(num_in_neighbors[x], -num_out_neighbors[x], -x),
             rev=true,
        ))
    end
    @assert reduce( & , allocated)
    @assert length(order) == length(vertices(graph))
    order
end

function make(recipes::Vector; maximum_running = 1, distributed = false)
    product2recipe = Dict{String, Int}()
    @info "gather products"
    for (v,recipe) in enumerate(recipes), product in recipe.products
        if product isa String
            if haskey(product2recipe, product)
                error("The product $product is produced by multiple recipes.")
            end
            product2recipe[product] = v
        else
            for prod in product
                if haskey(product2recipe, prod)
                    error("The product $prod is produced by multiple recipes.")
                end
                product2recipe[prod] = v
            end
        end
    end
    @info "make graph"
    dependencies = [ recipe.deps for recipe in recipes ]
    graph = make_graph(product2recipe, length(recipes), dependencies)
    @info "check for circular deps "
    if has_self_loops(graph)
        error("Circular dependencies detected")
    end

    @info "ordering recipes"
    order = recipe_order(graph)

    tasks = Dict{Int,Task}()
    @info "Checking recipes"
    @sync for v in order 
        recipe = recipes[v]
       #if recipe isa Time_Averaged_RMSD
       #    @show recipe.products
       #end
        states = [t.state for t in values(tasks)]
        abort = false
        while !abort && sum(states .!= :done) >= maximum_running 
            yield()
            states = [t.state for t in values(tasks)]
            abort = reduce( | , states .== :failed )
        end
        if abort
            @error "Failure detected. Aborting."
            break
        end
        for n in outneighbors(graph, v)
            if haskey(tasks,n)
                wait(tasks[n])
            end
        end
        #@info "scheduling task" 
        tasks[v] = @async begin
            #@info "task starting"
            if is_stale(recipe)
                @info "Making recipe" recipe
                for product in recipe.products
                    if product isa String
                        mkpath(dirname(product))
                    else
                        for p in product
                            mkpath(dirname(p))
                        end
                    end
                end
                if distributed
                    fetch( @spawn make(recipe) )
                else
                    make(recipe)
                end
                @info "Made recipe" recipe
            end
            is_stale(recipe, err=true)
            #@info "Task ending"
        end
        states = [t.state for t in values(tasks)]
        if :failed in states
            @error "One or more recipes have errored."
            errored = true
            break
        end
    end
end

end
