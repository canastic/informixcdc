package informixcdc

interface MyDep {
	fun whatever(i: Int)
}

fun hello(d: MyDep) {
    d.whatever(123)
}
