use std::collections::HashSet;

use futures::{TryStreamExt, future::try_join};
use tuwunel_core::{
	Result,
	utils::stream::{IterStream, TryBroadbandExt},
};

use crate::admin_command;

#[admin_command]
pub(super) async fn query_storage_sync(&self, src: String, dst: String) -> Result {
	let src_p = self.services.storage.provider(&src)?;

	let dst_p = self.services.storage.provider(&dst)?;

	let src = src_p
		.list(None)
		.map_ok(|meta| meta.location)
		.try_collect::<HashSet<_>>();

	let dst = dst_p
		.list(None)
		.map_ok(|meta| meta.location)
		.try_collect::<HashSet<_>>();

	let (src, dst) = try_join(src, dst).await?;

	src.difference(&dst)
		.try_stream()
		.broadn_and_then(2, async |item| {
			let data = src_p.get(item.as_ref()).await?;
			let put = dst_p.put_one(item.as_ref(), data).await?;

			Ok((item, put))
		})
		.try_for_each(|(item, put)| {
			writeln!(&self, "Moved {item} from {src:?} to {dst:?}; {put:?}")
		})
		.await
}
