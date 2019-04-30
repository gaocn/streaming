package govind

case class CheckpointT(name: String) {
	override def toString: String = s"CheckpointT[${name}]"
}

class TestClassParam(cp_ : CheckpointT) {
	val cp: CheckpointT = {
		if (cp_ != null) {
			cp_
		} else {
			null
		}
	}

	val isCpPresent: Boolean = cp_ != null

	def initialCp: CheckpointT = {
		if (isCpPresent) cp_ else null
	}
}

object TestClassParam {
	def main(args: Array[String]): Unit = {
		val cp = new CheckpointT("Govind")
		val tcp = new TestClassParam(cp)

		assert(tcp.isCpPresent)
		assert(tcp.initialCp == cp)

	}
}