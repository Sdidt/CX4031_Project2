    select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as value
        from
        partsupp,
        supplier,
        nation
        where
        ps_suppkey = s_suppkey
        and not s_nationkey = n_nationkey
        and n_name = 'GERMANY'
        and ps_supplycost > 20
        and s_acctbal > 10
        group by
        ps_partkey having
            sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'GERMANY'
            ) as T
        order by
        value desc;