package org.apache.spark.streaming.dstream.state

class StateImpl[S] extends State[S] {
	private var state: S = null.asInstanceOf[S]
	private var defined: Boolean = false
	private var timingOut: Boolean = false
	private var updated: Boolean = false
	private var removed: Boolean = false

	override def exists(): Boolean = defined

	override def get(): S = {
		if (defined) {
			state
		} else {
			throw new NoSuchElementException("State is not set")
		}
	}

	override def update(newState: S): Unit = {
		require(!removed, "状态已经移除，无法更新")
		require(!timingOut, "状态即将过期，无法更新")
		state = newState
		defined = true
		updated  = true
	}


	override def remove(): Unit = {
		require(!timingOut, "状态即将过期，无法更新")
		require(!removed,  "状态已经移除，无法更新")
		defined = false
		updated = false
		removed = true
	}


	override def isTimingOut(): Boolean = timingOut


	def isRemoved(): Boolean = removed
	def isUpdated(): Boolean = updated

	/**
		* Update the internal data and flags in `this` to the
		* given state that is going to be timed out.
		*
		* This method allows `this` object to be reused across
		* many state records.
		*/
	def wrap(optionalState: Option[S]):Unit = {
		optionalState match {
			case Some(newState) =>
				this.state = newState
				defined = true
			case None =>
				this.state = null.asInstanceOf[S]
				defined = false
		}
		timingOut = false
		removed = false
		updated = false
	}

	/**
		* Update the internal data and flags in `this` to the
		* given state that is going to be timed out.
		*
		* This method allows `this` object to be reused across
		* many state records.
		*/
	def wrapTimingOutState(newState: S):Unit = {
		this.state = newState
		defined = true
		timingOut = true
		removed = false
		updated = false
	}
}
