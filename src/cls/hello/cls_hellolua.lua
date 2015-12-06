function say_hello(input, output)
    output:append("Hello, ")
    if #input > 0 then
        output:append(input:str())
    else
        output:append("world")
    end
    output:append("! (from Lua)")
end

cls.register(say_hello)
