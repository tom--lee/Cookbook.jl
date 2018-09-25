
using Test

using Cookbook

@Recipe MyRecipe begin
    inputs = begin
        c, "Some text file"
        d..., "A series of text files"
    end
    outputs = begin
        a, "The text of c in reverse"
        b..., "The texts of d in reverse"
    end
    args = begin
        header::String, "A header line to print at the top of each file"
    end

    @assert length(outputs.b) == length(inputs.d)
    cstr = open(x->read(x, String), inputs.c)
    open(outputs.a, "w") do f
        print(f, args.header)
        print(f, reverse(cstr))
        print(f, "\n")
    end
    for (bi, di)  in zip(outputs.b,inputs.d)
        dstr = open(x->read(x, String), di)
        open(bi, "w") do f
            print(f, args.header)
            print(f, reverse(dstr))
            print(f, "\n")
        end
    end
end

for f in ["a", "b1", "b2", "c", "d1", "d2"]
    rm("test/$f.txt", force=true)
end

header="this is the first line"
recipes = Recipe[]
prepare!(
    recipes,
    MyRecipe,
    a="test/a.txt",
    b=["test/b1.txt", "test/b2.txt"],
    c="test/c.txt",
    d=["test/d1.txt", "test/d2.txt"],
    header=header,
)

c_content = "this is input \ntext file c"
open("test/c.txt", "w") do f
    println(f, c_content)
end
d_content =  [ "this is input \ntext file d$i" for i in 1:2 ]
for i in 1:2
    open("test/d$i.txt", "w") do f
        println(f, d_content[i])
    end
end

make(recipes)

@test "$header\n$(reverse(c_content))\n" == open(x->read(x, String), "test/a.txt")

for i in 1:2
    expect = "$header\n$(reverse(d_content[i]))\n"
    @test  expect == open(x->read(x, String), "test/b$i.txt")
end

make(recipes)


#@cook MyRecipe begin
#    c = "c.txt"
#    d = ["d1.txt", "d2.txt"]
#    a = "a.txt"
#    b = ["b1.txt", "b2.txt"]
#    header = "This is the first line"
#end


@test true


@test true
