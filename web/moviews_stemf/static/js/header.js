var header = new Vue({
        el: '#header',
        data() {
            return {
                maniulist: ['首页', '分类', '实时', '热门', '高评', '最新'],
                activehref: ['/recommon', '/movieslist', '/realtime', '/hot', '/height', '/new'],
                number: 0,
            }
        },
        methods: {
            active(abc) {
                this.number = abc
                console.log(abc)
            }
        }
    });