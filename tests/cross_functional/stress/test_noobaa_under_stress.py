from ocs_ci.helpers.mcg_stress_helper import upload_objs_to_buckets


class TestNoobaaUnderStress:

    base_setup_buckets = None

    def test_noobaa_under_stress(
        self,
        setup_stress_testing_bucket,
        nb_stress_cli_pod,
        mcg_obj_session,
        rgw_obj_session,
    ):

        self.base_setup_buckets = setup_stress_testing_bucket()

        upload_objs_to_buckets(
            mcg_obj_session,
            nb_stress_cli_pod,
            self.base_setup_buckets,
        )
