\name{litxr_add_ref_identity_pair}
\alias{litxr_add_ref_identity_pair}
\title{Add a strict arXiv/DOI identity pair}
\usage{
litxr_add_ref_identity_pair(arxiv_ref_id, doi, config = NULL)
}
\arguments{
\item{arxiv_ref_id}{Bare arXiv id or canonical \code{arxiv:} ref id.}
\item{doi}{Bare DOI, \code{doi:} ref id, or DOI URL.}
\item{config}{Optional parsed config list or a direct config path. When
omitted, \code{litxr} reads \code{LITXR_DATA_ROOT}.}
}
\value{
Named list describing the appended pair.
}
\description{
Append one canonical arXiv/DOI pair directly to the project identity map.
The pair is rejected if either surface id already exists anywhere in the
current identity map.
}
