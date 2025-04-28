use std::path::PathBuf;

use manifest_dir_macros::exist_relative_path;

pub enum GetDump {
    MoviesRawV1,
    MoviesWithSettingsV1,
    RubyGemsWithSettingsV1,

    MoviesRawV2,
    MoviesWithSettingsV2,
    RubyGemsWithSettingsV2,

    MoviesRawV3,
    MoviesWithSettingsV3,
    RubyGemsWithSettingsV3,

    MoviesRawV4,
    MoviesWithSettingsV4,
    RubyGemsWithSettingsV4,

    TestV5,

    TestV6WithExperimental,
    TestV6WithBatchesAndEnqueuedTasks,
}

impl GetDump {
    pub fn path(&self) -> PathBuf {
        match self {
            Self::MoviesRawV1 => exist_relative_path!("tests/assets/v1_v0.20.0_movies.dump").into(),
            Self::MoviesWithSettingsV1 => {
                exist_relative_path!("tests/assets/v1_v0.20.0_movies_with_settings.dump").into()
            }
            Self::RubyGemsWithSettingsV1 => {
                exist_relative_path!("tests/assets/v1_v0.20.0_rubygems_with_settings.dump").into()
            }

            Self::MoviesRawV2 => exist_relative_path!("tests/assets/v2_v0.21.1_movies.dump").into(),
            Self::MoviesWithSettingsV2 => {
                exist_relative_path!("tests/assets/v2_v0.21.1_movies_with_settings.dump").into()
            }

            Self::RubyGemsWithSettingsV2 => {
                exist_relative_path!("tests/assets/v2_v0.21.1_rubygems_with_settings.dump").into()
            }

            Self::MoviesRawV3 => exist_relative_path!("tests/assets/v3_v0.24.0_movies.dump").into(),
            Self::MoviesWithSettingsV3 => {
                exist_relative_path!("tests/assets/v3_v0.24.0_movies_with_settings.dump").into()
            }
            Self::RubyGemsWithSettingsV3 => {
                exist_relative_path!("tests/assets/v3_v0.24.0_rubygems_with_settings.dump").into()
            }

            Self::MoviesRawV4 => exist_relative_path!("tests/assets/v4_v0.25.2_movies.dump").into(),
            Self::MoviesWithSettingsV4 => {
                exist_relative_path!("tests/assets/v4_v0.25.2_movies_with_settings.dump").into()
            }
            Self::RubyGemsWithSettingsV4 => {
                exist_relative_path!("tests/assets/v4_v0.25.2_rubygems_with_settings.dump").into()
            }
            Self::TestV5 => exist_relative_path!("tests/assets/v5_v0.28.0_test_dump.dump").into(),
            Self::TestV6WithExperimental => exist_relative_path!(
                "tests/assets/v6_v1.6.0_use_deactivated_experimental_setting.dump"
            )
            .into(),
            Self::TestV6WithBatchesAndEnqueuedTasks => {
                exist_relative_path!("tests/assets/v6_v1.13.0_batches_and_enqueued_tasks.dump")
                    .into()
            }
        }
    }
}
